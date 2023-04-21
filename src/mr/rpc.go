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
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

const (
	READY_FOR_MAPPING = iota // 有map任务，可以进行map
	MAPPING
	READY_FOR_REDUCING // 有reduce任务，可以进行reduce
	REDUCING
	COMPLETED // 所有reduce任务都被分配，空闲
)

const (
	IDLE_WORKER = iota
	MAPPING_WORKER
	REDUCING_WORKER
)

const (
	INVALID_WORKER_ID = -1
	REMOVED_WORKER    = -2
)

const (
	INVALID_TASK_ID = -1
)

type RequestArgs struct {
	WorkerId int
	status   int
}

type RequestReply struct {
	WorkerId int
	Task     Task
	NReduce  int
}
