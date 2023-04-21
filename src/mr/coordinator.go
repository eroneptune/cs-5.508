package mr

import (
	"errors"
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
	mapList     []TaskInfo
	mapMutex    sync.Mutex
	reduceList  []TaskInfo
	reduceMutex sync.Mutex

	workerList  []WorkerInfo
	MaxWorkerId int
	workerMutex sync.Mutex

	mrStatus    int
	statusMutex sync.Mutex

	nReduce int
	fileSum int
	ticker  *time.Ticker
}

type WorkerInfo struct {
	WorkerId      int
	Status        int
	CurrentTaskId int
	Timestamp     int64
	mutex         sync.Mutex
}

type Task struct {
	TaskId   int
	ReduceId int
	FileSum  int
	Status   int
	FileName string
}

type TaskInfo struct {
	Task  Task
	mutex sync.Mutex
}

func (c *Coordinator) Call(args *RequestArgs, reply *RequestReply) error {
	worker := c.fetchWorker(args.WorkerId)
	if worker == nil {
		return errors.New("Invalid WorkerId")
	}
	worker.Timestamp = time.Now().Unix()

	if worker.CurrentTaskId != INVALID_TASK_ID {
		e := c.updateWorkerAndTask(worker)
		if e != nil {
			worker.mutex.Unlock()
			return e
		}
	}
	task := c.allocTask()
	if task == nil {
		// TODO
		reply.NReduce = -1
		reply.WorkerId = worker.WorkerId
		worker.mutex.Unlock()
		return nil
	}
	worker.CurrentTaskId = task.Task.TaskId

	reply.WorkerId = worker.WorkerId
	reply.Task = task.Task
	reply.NReduce = c.nReduce

	task.mutex.Unlock()
	worker.mutex.Unlock()
	return nil
}

func (c *Coordinator) Register(args *RequestArgs, reply *RequestReply) error {
	c.workerMutex.Lock()
	defer c.workerMutex.Unlock()

	worker := WorkerInfo{c.MaxWorkerId, IDLE_WORKER, INVALID_TASK_ID, time.Now().Unix(), sync.Mutex{}}
	c.workerList = append(c.workerList, worker)
	reply.WorkerId = worker.WorkerId
	c.MaxWorkerId += 1

	return nil
}

func (c *Coordinator) PingPong(args *RequestArgs, reply *RequestReply) error {
	worker := c.fetchWorker(args.WorkerId)
	if worker == nil {
		return errors.New("Invalid WorkerId")
	}
	worker.Timestamp = time.Now().Unix()
	worker.mutex.Unlock()
	return nil
}

func (c *Coordinator) Timeout() {
	c.workerMutex.Lock()

	for index := range c.workerList {
		if c.workerList[index].WorkerId != REMOVED_WORKER && c.workerList[index].Timestamp+10 < time.Now().Unix() {
			c.workerList[index].mutex.Lock()
			c.ReleaseTask(c.workerList[index].CurrentTaskId)
			c.workerList[index].WorkerId = REMOVED_WORKER
			c.workerList[index].mutex.Unlock()
		}
	}
	c.workerMutex.Unlock()
}

func (c *Coordinator) ReleaseTask(taskId int) {
	if taskId == INVALID_TASK_ID {
		return
	}

	c.statusMutex.Lock()

	if c.mrStatus == MAPPING_WORKER {
		c.mapMutex.Lock()
		c.statusMutex.Unlock()
		defer c.mapMutex.Unlock()

		for index := range c.mapList {
			if c.mapList[index].Task.TaskId == taskId {
				c.mapList[index].mutex.Lock()
				c.mapList[index].Task.Status = READY_FOR_MAPPING
				c.mapList[index].mutex.Unlock()
				return
			}
		}

	} else if c.mrStatus == REDUCING_WORKER {
		c.reduceMutex.Lock()
		c.statusMutex.Unlock()
		defer c.reduceMutex.Unlock()

		for index := range c.reduceList {
			if c.reduceList[index].Task.TaskId == taskId {
				c.reduceList[index].mutex.Lock()
				c.reduceList[index].Task.Status = READY_FOR_REDUCING
				c.reduceList[index].mutex.Unlock()
				return
			}
		}

	}
}

func (c *Coordinator) updateWorkerAndTask(worker *WorkerInfo) error {
	c.statusMutex.Lock()
	defer c.statusMutex.Unlock()

	if c.mrStatus == MAPPING_WORKER {
		index := -1
		c.mapMutex.Lock()
		defer c.mapMutex.Unlock()

		for i := range c.mapList {
			if c.mapList[i].Task.TaskId == worker.CurrentTaskId {
				index = i
				break
			}
		}
		// task not exists
		if index == -1 {

			return errors.New("Invalid TaskId")
		}

		c.mapList = append(c.mapList[:index], c.mapList[(index+1):]...)
		if len(c.mapList) == 0 {
			// map phase complete
			c.createReduceTask()
			c.mrStatus = REDUCING_WORKER
		}
	} else if c.mrStatus == REDUCING_WORKER {
		index := -1
		c.reduceMutex.Lock()
		defer c.reduceMutex.Unlock()

		for i := range c.reduceList {
			if c.reduceList[i].Task.TaskId == worker.CurrentTaskId {
				index = i
				break
			}
		}
		// task not exists
		if index == -1 {

			return errors.New("Invalid TaskId")
		}

		c.reduceList = append(c.reduceList[:index], c.reduceList[(index+1):]...)
		if len(c.reduceList) == 0 {
			// reduce phase complete
			c.mrStatus = IDLE_WORKER
		}
	}

	worker.CurrentTaskId = INVALID_TASK_ID

	return nil
}

func (c *Coordinator) fetchWorker(workerId int) *WorkerInfo {
	c.workerMutex.Lock()
	defer c.workerMutex.Unlock()

	for index := range c.workerList {
		if c.workerList[index].WorkerId == workerId {
			c.workerList[index].mutex.Lock()
			return &c.workerList[index]
		}
	}
	return nil
}

func (c *Coordinator) allocTask() *TaskInfo {
	c.statusMutex.Lock()

	// need refactor
	if c.mrStatus == MAPPING_WORKER {
		c.mapMutex.Lock()
		c.statusMutex.Unlock()
		defer c.mapMutex.Unlock()

		for index := range c.mapList {
			if c.mapList[index].Task.Status == READY_FOR_MAPPING {
				c.mapList[index].mutex.Lock()
				c.mapList[index].Task.Status = MAPPING
				return &c.mapList[index]
			}
		}
		return nil
	} else if c.mrStatus == REDUCING_WORKER {
		c.reduceMutex.Lock()
		c.statusMutex.Unlock()
		defer c.reduceMutex.Unlock()

		for index := range c.reduceList {
			if c.reduceList[index].Task.Status == READY_FOR_REDUCING {
				c.reduceList[index].mutex.Lock()
				c.reduceList[index].Task.Status = REDUCING
				return &c.reduceList[index]
			}
		}
		return nil
	}
	c.statusMutex.Unlock()
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
	c.MaxWorkerId = 0
	c.mrStatus = MAPPING_WORKER
	c.fileSum = len(files)
	// Your code here.

	c.createMapTask(files)
	c.server()
	c.ticker = time.NewTicker(1 * time.Second)
	go c.CoordinatorHandler(c.ticker)
	return &c
}

func (c *Coordinator) CoordinatorHandler(ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
			go c.Timeout()
		}
	}
}

func (c *Coordinator) createMapTask(files []string) {
	c.mapMutex.Lock()
	for index, file := range files {
		task := TaskInfo{Task{index, -1, -1, READY_FOR_MAPPING, file}, sync.Mutex{}}
		c.mapList = append(c.mapList, task)
	}
	c.mapMutex.Unlock()
}

func (c *Coordinator) createReduceTask() {
	for index := 0; index < c.nReduce; index++ {
		task := TaskInfo{Task{index, index, c.fileSum, READY_FOR_REDUCING, ""}, sync.Mutex{}}
		c.reduceList = append(c.reduceList, task)
	}
}
