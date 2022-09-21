package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type RequestJobArgs struct {
	WorkerId int
}

type RequestJobReply struct {
	TaskId      int
	Type        TaskType
	File        string
	ReduceCount int
}

type ReportJobArgs struct {
	TaskId   int
	WorkerId int
}

type ReportJobReply struct{}

type HeartbeatArgs struct {
	WorkerId int
}

type HeartbeatReply struct{}

// Add your RPC definitions here.
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	s += ".sock"
	return s
}
