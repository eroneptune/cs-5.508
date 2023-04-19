package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapList       []Task
	mapMutex      sync.Mutex
	reduceList    []Task
	reduceMutex   sync.Mutex
	completeList  []Task
	completeMutex sync.Mutex

	workerList  []WorkerInfo
	workerMutex sync.Mutex
	maxWorkerId int
	IdMutex     sync.Mutex

	nReduce  int
	mrStatus int
	fileSums int
}

type WorkerInfo struct {
	WorkerId      int
	Status        int
	CurrentTaskId int
	timestamp     int64
}

type Task struct {
	TaskId   int
	ReduceId int
	FileSum  int
	Status   int
	FileName string
}

func (c *Coordinator) Call(args *RequestArgs, reply *RequestReply) error {
	worker := c.fetchWorker(args.WorkerId)
	if worker == nil {
		return errors.New("Invalid WorkerId")
	}
	worker.timestamp = time.Now().Unix()

	if worker.CurrentTaskId != INVALID_TASK_ID {
		e := c.updateWorkerAndTask(worker)
		if e != nil {
			return e
		}
	}
	task := c.allocTask()
	if task == nil {
		// TODO
		reply.NReduce = -1
		reply.Worker = *worker
		return nil
	}
	worker.CurrentTaskId = task.TaskId
	worker.Status = c.mrStatus

	reply.Worker = *worker
	reply.Task = *task
	reply.NReduce = c.nReduce
	if worker.Status == REDUCING_WORKER {
		fmt.Println(reply.Task.TaskId, reply.Task.ReduceId, reply.Task.FileSum)
		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

func (c *Coordinator) Register(args *RequestArgs, reply *RequestReply) error {
	worker := WorkerInfo{c.maxWorkerId, IDLE_WORKER, INVALID_TASK_ID, time.Now().Unix()}
	c.workerList = append(c.workerList, worker)
	c.maxWorkerId += 1
	reply.Worker = worker
	return nil
}

func (c *Coordinator) PingPong(args *RequestArgs, reply *RequestReply) error {
	worker := c.fetchWorker(args.WorkerId)
	if worker == nil {
		return errors.New("Invalid WorkerId")
	}
	worker.timestamp = time.Now().Unix()
	return nil
}

func (c *Coordinator) updateWorkerAndTask(worker *WorkerInfo) error {
	task := c.fetchTask(worker.CurrentTaskId)
	if task == nil {
		return errors.New("Invalid TaskId")
	}

	worker.Status = IDLE_WORKER
	worker.CurrentTaskId = INVALID_TASK_ID

	if task.Status == MAPPING {
		task.Status = READY_FOR_REDUCING
		c.mapToReduce(task.TaskId)
	} else if task.Status == REDUCING {
		task.Status = COMPLETED
		c.reduceToComplete(task.TaskId)
	}

	return nil
}

func (c *Coordinator) mapToReduce(taskId int) {
	var index int
	for i, task := range c.mapList {
		if task.TaskId == taskId {
			index = i
			break
		}
	}

	c.mapList = append(c.mapList[:index], c.mapList[(index+1):]...)
	if len(c.mapList) == 0 {
		c.createReduceTask()
		c.mrStatus = REDUCING_WORKER
		fmt.Println("finish")
	}
}

func (c *Coordinator) reduceToComplete(taskId int) {
	var index int
	for i, task := range c.reduceList {
		if task.TaskId == taskId {
			index = i
			break
		}
	}

	c.reduceList = append(c.reduceList[:index], c.reduceList[(index+1):]...)
	if len(c.reduceList) == 0 {
		c.mrStatus = IDLE_WORKER
	}
}

func (c *Coordinator) fetchWorker(workerId int) *WorkerInfo {
	for index := range c.workerList {
		if c.workerList[index].WorkerId == workerId {
			return &c.workerList[index]
		}
	}
	return nil
}

func (c *Coordinator) fetchTask(taskId int) *Task {
	if c.mrStatus == MAPPING_WORKER {
		for index := range c.mapList {
			if c.mapList[index].TaskId == taskId {
				return &c.mapList[index]
			}
		}
	} else if c.mrStatus == REDUCING_WORKER {
		for index := range c.reduceList {
			if c.reduceList[index].TaskId == taskId {
				return &c.reduceList[index]
			}
		}
	}

	return nil
}

func (c *Coordinator) allocTask() *Task {
	// need refactor
	if c.mrStatus == MAPPING_WORKER {
		for index := range c.mapList {
			if c.mapList[index].Status == READY_FOR_MAPPING {
				c.mapList[index].Status = MAPPING
				return &c.mapList[index]
			}
		}
	} else if c.mrStatus == REDUCING_WORKER {
		for index := range c.reduceList {
			if c.reduceList[index].Status == READY_FOR_REDUCING {
				c.reduceList[index].Status = REDUCING
				return &c.reduceList[index]
			}
		}
	}

	return nil
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
	// Your code here.
	return c.mrStatus == IDLE_WORKER
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.maxWorkerId = 0
	c.mrStatus = MAPPING_WORKER
	c.fileSums = len(files)
	// Your code here.
	c.createMapTask(files)
	c.server()
	return &c
}

func (c *Coordinator) createMapTask(files []string) {
	for index, file := range files {
		task := Task{index, -1, -1, READY_FOR_MAPPING, file}
		c.mapList = append(c.mapList, task)
	}
}

func (c *Coordinator) createReduceTask() {
	for index := 0; index < c.nReduce; index++ {
		task := Task{index, index, c.fileSums, READY_FOR_REDUCING, ""}
		c.reduceList = append(c.reduceList, task)
	}
}
