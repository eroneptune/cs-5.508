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

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840.1-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

const (
	TaskStatusCreated = iota
	TaskStatusRunning
	TaskStatusCompleted
)

const (
	MRStatusMapping = iota
	MRStatusReducing
	MRStatusCompleted
)

const (
	TaskTypeNone = iota
	TaskTypeMap
	TaskTypeReduce
)

type RequestForTaskArgs struct {
	TaskId int
	Status int
}

type RequestForTaskReply struct {
	Task
	Ttype int
	Done  bool
}

type PingArgs struct {
	TaskId int
}

type PingReply struct {
}
