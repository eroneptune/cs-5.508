package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type NoOutput struct {
}

func (n *NoOutput) Write(b []byte) (int, error) {
	return 0, nil
}

func init() {
	log.SetOutput(&NoOutput{})
}

type Coordinator struct {
	mapTasks chan *Task
	allTasks map[int]*Task
	mu       sync.RWMutex

	files     []string
	maxTaskId int

	nReduce  int
	mrStatus int

	ticker *time.Ticker
}

type Task struct {
	TaskId     int
	MapId      int
	ReduceId   int
	Status     int
	NFiles     int
	NReduce    int
	FileName   string
	LastActive int64
}

func (c *Coordinator) releaseTask(taskId int) int {
	res := c.mrStatus
	delete(c.allTasks, taskId)
	if len(c.allTasks) == 0 {
		if c.mrStatus == MRStatusMapping {
			c.mrStatus = MRStatusReducing
			log.Printf("[Coordinator] Set c to reducing phase\n")
			c.createReduceTasks()
			res = MRStatusReducing
		} else if c.mrStatus == MRStatusReducing {
			c.mrStatus = MRStatusCompleted
			log.Println("[Coordinator] Done")
			// cleanup
			close(c.mapTasks)
			res = MRStatusCompleted
		}
	}

	return res
}

func (c *Coordinator) RequestForTask(args *RequestForTaskArgs, reply *RequestForTaskReply) error {
	if c.mrStatus == MRStatusCompleted {
		reply.Done = true
		return nil
	}
	if args.Status == TaskStatusCompleted {
		c.mu.Lock()
		status := c.releaseTask(args.TaskId)
		c.mu.Unlock()
		if status == MRStatusCompleted {
			reply.Done = true
			return nil
		}
	}

	if task, ok := <-c.mapTasks; ok {
		c.mu.Lock()
		if _, ok = c.allTasks[task.TaskId]; ok {
			task.LastActive = time.Now().Unix()
			task.Status = TaskStatusRunning
			reply.Task = *task
			if task.FileName == "" { // 最好直接使用type来区分
				reply.Ttype = TaskTypeReduce
			} else {
				reply.Ttype = TaskTypeMap
			}
		}
		c.mu.Unlock()
	} else {
		reply.Ttype = TaskTypeNone
	}

	return nil
}

func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskId := args.TaskId
	if task, ok := c.allTasks[taskId]; ok {
		task.LastActive = time.Now().Unix()
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
	return c.mrStatus == MRStatusCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nfiles := len(files)
	c := &Coordinator{
		mapTasks:  make(chan *Task, nfiles+10),
		allTasks:  make(map[int]*Task),
		nReduce:   nReduce,
		maxTaskId: 0,
		files:     files,
		mrStatus:  MRStatusMapping,
		ticker:    time.NewTicker(3 * time.Second),
	}

	c.createMapTasks()
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.cleanIdleTasks()
			}
		}
	}()
	c.server()
	return c
}

func (c *Coordinator) cleanIdleTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	for _, t := range c.allTasks {
		if t.Status == TaskStatusRunning && now-t.LastActive > 10 {
			log.Printf("[Coordinator] Task %d timeout\n", t.TaskId)
			t.Status = TaskStatusCreated
			c.mapTasks <- t
		}
	}
}

func (c *Coordinator) createMapTasks() {
	files := c.files
	for i := range files {
		task := &Task{
			TaskId:   c.maxTaskId,
			MapId:    i,
			NReduce:  c.nReduce,
			NFiles:   len(files),
			FileName: files[i],
			Status:   TaskStatusCreated,
		}
		c.maxTaskId++

		c.mapTasks <- task
		c.allTasks[task.TaskId] = task
	}
}

func (c *Coordinator) createReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		task := &Task{
			TaskId:   c.maxTaskId,
			ReduceId: i,
			NFiles:   len(c.files),
			NReduce:  c.nReduce,
			Status:   TaskStatusCreated,
		}
		c.maxTaskId++

		c.mapTasks <- task
		c.allTasks[task.TaskId] = task
	}
}
