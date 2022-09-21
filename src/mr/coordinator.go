package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	mapTasks         []*Task
	reduceTasks      []*Task
	mapTasksDone     int
	reduceTasksDone  int
	nReduce          int
	mu               sync.Mutex
	latestHeartbeats map[int]time.Time
}

// RPC Handler for Coordinator.RequestJob
func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.latestHeartbeats[args.WorkerId] = time.Now()

	// If not all map tasks are done
	if c.mapTasksDone < len(c.mapTasks) {
		assignIdleTask(c.mapTasks, c.nReduce, args, reply)
		return nil
	}

	// If all map tasks are done, distribute reduce tasks
	if c.mapTasksDone == len(c.mapTasks) && c.reduceTasksDone < len(c.reduceTasks) {
		assignIdleTask(c.reduceTasks, c.nReduce, args, reply)
		return nil
	}

	if c.mapTasksDone == len(c.mapTasks) && c.reduceTasksDone == len(c.reduceTasks) {
		reply.Type = Exit
		return nil
	}

	return nil
}

func assignIdleTask(tasks []*Task, nReduce int, args *RequestJobArgs, reply *RequestJobReply) {
	// Note: a lock is being held

	for _, task := range tasks {
		if task.State == Idle {
			reply.TaskId = task.Id
			reply.Type = task.Type
			reply.File = task.File
			reply.ReduceCount = nReduce

			task.State = InProgress
			task.WorkerId = args.WorkerId

			fmt.Printf("Assigned task %d with type %d to worker %d\n", task.Id, task.Type, task.WorkerId)
			return
		}
	}

	reply.Type = Sleep
}

// RPC Handler for Coordinator.ReportMapJob
func (c *Coordinator) ReportMapJob(args *ReportJobArgs, reply *ReportJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := c.mapTasks[args.TaskId]

	// Check if task completion is valid
	if args.WorkerId == task.WorkerId && task.State == InProgress {
		// Complete task
		task.State = Completed
		c.mapTasksDone += 1
		fmt.Printf("Task %d done by worker %d\n", args.TaskId, args.WorkerId)
	}

	return nil
}

// RPC Handler for Coordinator.ReportJob
func (c *Coordinator) ReportReduceJob(args *ReportJobArgs, reply *ReportJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := c.reduceTasks[args.TaskId]

	// Check if task completion is valid
	if args.WorkerId == task.WorkerId && task.State == InProgress {
		// Complete task
		task.State = Completed
		c.reduceTasksDone += 1
		fmt.Printf("Task %d done by worker %d\n", args.TaskId, args.WorkerId)

		// Cleanup all intermediate files with the current reducer id
		cwd, err := os.Getwd()
		handleError(err)

		globpath := fmt.Sprintf("%s/mr-tmp-*-%d.txt", cwd, args.TaskId)

		files, err := filepath.Glob(globpath)
		handleError(err)

		for _, f := range files {
			err := os.Remove(f)
			handleError(err)
		}
	}

	return nil
}

// RPC Handler for Coordinator.Heartbeat
func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.latestHeartbeats[args.WorkerId] = time.Now()

	return nil
}

func (c *Coordinator) checkForTimeouts() {
	maxDelay := -5 * time.Second
	minimumLatest := time.Now().Add(maxDelay)
	c.mu.Lock()
	defer c.mu.Unlock()

	// Map Timeouts

	for workerId, latestHeartbeat := range c.latestHeartbeats {
		if minimumLatest.After(latestHeartbeat) {
			c.handleWorkerTimeout(workerId)
			delete(c.latestHeartbeats, workerId)
		}
	}

}

func (c *Coordinator) handleWorkerTimeout(workerId int) {
	// If worker was in map phase
	if c.mapTasksDone < len(c.mapTasks) {
		fmt.Printf("Worker %d timed out during map phase\n", workerId)

		for _, mapTask := range c.mapTasks {
			if mapTask.WorkerId == workerId {
				// Reset done count
				if mapTask.State == Completed {
					c.mapTasksDone -= 1
				}

				// Resetting map task
				mapTask.WorkerId = -1
				mapTask.State = Idle

				// Delete created fies
				cwd, err := os.Getwd()
				handleError(err)

				globpath := fmt.Sprintf("%s/mr-tmp-%d-*.txt", cwd, workerId)

				files, err := filepath.Glob(globpath)
				handleError(err)

				for _, f := range files {
					err := os.Remove(f)
					handleError(err)
				}
			}
		}
	}

	// If worker was in reduce phase
	if c.mapTasksDone == len(c.mapTasks) && c.reduceTasksDone < len(c.reduceTasks) {
		fmt.Printf("Worker %d timed out during reduce phase\n", workerId)

		for _, reduceTask := range c.reduceTasks {
			if reduceTask.WorkerId == workerId {
				if reduceTask.State == Completed {
					// Completed reduce tasks do not have to be restarted
					continue
				}

				// Resetting map task
				reduceTask.WorkerId = -1
				reduceTask.State = Idle
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	isDone := len(c.mapTasks) == c.mapTasksDone && len(c.reduceTasks) == c.reduceTasksDone

	if isDone {
		fmt.Println("MapReduce finished, exiting...")
	}

	return isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("Starting mrcoordinator...")

	mapTaskCount := len(files)

	c := Coordinator{
		mapTasks:         make([]*Task, 0, mapTaskCount),
		reduceTasks:      make([]*Task, 0, nReduce),
		latestHeartbeats: make(map[int]time.Time),
		nReduce:          nReduce,
		mapTasksDone:     0,
		reduceTasksDone:  0,
	}

	// Generate Map Tasks
	for idx, file := range files {
		t := &Task{
			Id:       idx,
			Type:     MapTask,
			State:    Idle,
			File:     file,
			WorkerId: -1,
		}

		c.mapTasks = append(c.mapTasks, t)
	}

	// Generate Reducer Tasks
	for i := 0; i < nReduce; i++ {
		t := &Task{
			Id:       i,
			Type:     ReduceTask,
			State:    Idle,
			WorkerId: -1,
		}

		c.reduceTasks = append(c.reduceTasks, t)
	}

	c.server()

	// Run timeout checker
	go func() {
		for range time.Tick(time.Second) {
			c.checkForTimeouts()
		}
	}()

	return &c
}
