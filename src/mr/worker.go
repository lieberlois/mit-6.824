package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := os.Getpid()

	go func() {
		for range time.Tick(time.Second) {
			sendHeartbeat(workerId)
		}
	}()

	for {
		job, err := requestJob(workerId)
		handleError(err)

		// Dummy sleep
		// time.Sleep(time.Second)

		switch job.Type {
		case MapTask:
			fmt.Printf("Received map task %d for file %s\n", job.TaskId, job.File)
			runMapTask(job, workerId, mapf)
		case ReduceTask:
			fmt.Printf("Received reduce task %d\n", job.TaskId)
			runReduceTask(job, workerId, reducef)
		case Exit:
			fmt.Println("Received exit command, exiting...")
			os.Exit(0)
		case Sleep:
			fmt.Println("No commands available, retrying in 1 second...")
			time.Sleep(time.Second)
		default:
			panic("Unknown task type")
		}
	}

}

func runMapTask(requestedJob *RequestJobReply, workerId int, mapf func(string, string) []KeyValue) {
	cwd, err := os.Getwd()
	handleError(err)

	path := fmt.Sprintf("%s/%s", cwd, requestedJob.File)

	// Read input file
	file, err := os.Open(path)
	handleError(err)

	content, err := ioutil.ReadAll(file)
	handleError(err)

	file.Close()

	// Run map function
	kva := mapf(path, string(content))

	// Create files
	files := make([]*os.File, 0, requestedJob.ReduceCount)
	buffers := make([]*bufio.Writer, 0, requestedJob.ReduceCount)

	for i := 0; i < requestedJob.ReduceCount; i++ {
		path := fmt.Sprintf("%s/mr-tmp-%d-%d.txt", cwd, workerId, i)

		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		handleError(err)

		buffer := bufio.NewWriter(file)

		files = append(files, file)
		buffers = append(buffers, buffer)
	}

	// Write to files
	for _, kv := range kva {
		// Distribute across nReducers by hash(key) % nReducer
		reducerIdx := ihash(kv.Key) % requestedJob.ReduceCount

		_, err = fmt.Fprintf(buffers[reducerIdx], "%v|%v\n", kv.Key, kv.Value)
		handleError(err)
	}

	// Flush buffers
	for _, buffer := range buffers {
		err := buffer.Flush()
		handleError(err)
	}

	// Close files
	for _, file := range files {
		file.Close()
	}

	// Notify Coordinator
	reportMapJobDone(requestedJob.TaskId, workerId)
}

// RPC call to Coordinator.RequestJob
func requestJob(workerId int) (*RequestJobReply, error) {
	args := &RequestJobArgs{
		WorkerId: workerId,
	}

	reply := &RequestJobReply{}

	ok := call("Coordinator.RequestJob", args, reply)
	if ok {
		return reply, nil
	} else {
		return nil, errors.New("RPC call Coordinator.RequestJob failed")
	}
}

// RPC call to Coordinator.ReportMapJob
func reportMapJobDone(taskId int, workerId int) (*ReportJobReply, error) {
	args := &ReportJobArgs{
		TaskId:   taskId,
		WorkerId: workerId,
	}

	reply := &ReportJobReply{}

	ok := call("Coordinator.ReportMapJob", args, reply)
	if ok {
		return reply, nil
	} else {
		return nil, errors.New("RPC call Coordinator.ReportMapJob failed")
	}
}

func runReduceTask(requestedJob *RequestJobReply, workerId int, reducef func(string, []string) string) {
	cwd, err := os.Getwd()
	handleError(err)

	paths, err := filepath.Glob(fmt.Sprintf("%v/mr-tmp-%v-%v.txt", cwd, "*", requestedJob.TaskId))
	handleError(err)

	kvMap := make(map[string][]string)

	// Build KV map
	for _, path := range paths {
		f, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
		handleError(err)

		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				return
			}
			data := strings.Split(line, "|")

			if len(data) != 2 {
				fmt.Printf("WRONG DATA: %v when reading file %s", data, path)
			}

			key := data[0]
			value := data[1]
			kvMap[key] = append(kvMap[key], value)
		}

		f.Close()
	}

	// Reduce and write to output
	path := fmt.Sprintf("%s/mr-out-%d.txt", cwd, requestedJob.TaskId)

	if _, err := os.Stat(path); err == nil {
		// Remove if already exists
		os.Remove(path)
	}

	file, err := os.Create(path)
	handleError(err)

	buffer := bufio.NewWriter(file)

	for k, v := range kvMap {
		res := reducef(k, v)
		fmt.Fprintf(buffer, "%v %v\n", k, res)
		buffer.Flush()
	}
	file.Close()

	// Notify Coordinator
	reportReduceJobDone(requestedJob.TaskId, workerId)
}

// RPC call to Coordinator.ReportReduceJob
func reportReduceJobDone(taskId int, workerId int) (*ReportJobReply, error) {
	args := &ReportJobArgs{
		TaskId:   taskId,
		WorkerId: workerId,
	}

	reply := &ReportJobReply{}

	ok := call("Coordinator.ReportReduceJob", args, reply)
	if ok {
		return reply, nil
	} else {
		return nil, errors.New("RPC call Coordinator.ReportReduceJob failed")
	}
}

// RPC call to Coordinator.ReportReduceJob
func sendHeartbeat(workerId int) error {
	args := &HeartbeatArgs{
		WorkerId: workerId,
	}

	reply := &ReportJobReply{}

	ok := call("Coordinator.Heartbeat", args, reply)
	if ok {
		return nil
	} else {
		return errors.New("RPC call Coordinator.Heartbeat failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	handleError(err)

	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
