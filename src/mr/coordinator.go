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

const MaxInactiveInSecond = 10

type Coordinator struct {
	// 存放Coordinator等待worker获取的任务队列
	// 包含Map任务以及Reduce任务
	taskQueue chan *Task
	// TaskId -> Task的映射，主要处理任务超时
	allTasks map[int]*Task
	// 对allTasks的锁
	mu sync.RWMutex

	// 等待分配的TaskId
	maxTaskId int

	nFiles   int
	nReduce  int
	mrStatus int

	ticker *time.Ticker
}

type Task struct {
	// 标识任务的id
	TaskId int
	// 任务状态
	Status int
	// 总文件数量
	NFiles int
	// reduce数量
	NReduce int
	// 任务最后活跃时间
	LastActive int64

	// Map任务的id
	// mr-#{MapId}-#{ReduceId}
	MapId int
	// Map任务读入的文件名
	FileName string

	// Reduce任务的id
	// mr-#{MapId}-#{ReduceId}
	ReduceId int
}

func (c *Coordinator) releaseTask(taskId int) int {
	res := c.mrStatus
	delete(c.allTasks, taskId)
	if len(c.allTasks) == 0 {
		if c.mrStatus == MRStatusMapping {
			// Map任务结束，转入Reduce
			c.mrStatus = MRStatusReducing
			log.Printf("[Coordinator] Set c to reducing phase\n")
			c.createReduceTasks()
			res = MRStatusReducing
		} else if c.mrStatus == MRStatusReducing {
			// Reduce任务结束，整个任务结束
			c.mrStatus = MRStatusCompleted
			log.Println("[Coordinator] Done")
			// cleanup
			close(c.taskQueue)
			res = MRStatusCompleted
		}
	}

	return res
}

func (c *Coordinator) RequestForTask(args *RequestForTaskArgs, reply *RequestForTaskReply) error {
	// 所有任务都完成
	if c.mrStatus == MRStatusCompleted {
		reply.Done = true
		return nil
	}

	// Worker通知Coordinator任务完成，释放任务并更新新任务
	if args.Status == TaskStatusCompleted {
		c.mu.Lock()
		status := c.releaseTask(args.TaskId)
		c.mu.Unlock()
		if status == MRStatusCompleted {
			reply.Done = true
			return nil
		}
	}

	if task, ok := <-c.taskQueue; ok {
		c.mu.Lock()
		// 确保任务存在于allTasks中
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
	c := &Coordinator{
		taskQueue: make(chan *Task, len(files)+10),
		allTasks:  make(map[int]*Task),
		nReduce:   nReduce,
		maxTaskId: 0,
		nFiles:    len(files),
		mrStatus:  MRStatusMapping,
		ticker:    time.NewTicker(3 * time.Second),
	}

	c.createMapTasks(files)
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
		if t.Status == TaskStatusRunning && now-t.LastActive > MaxInactiveInSecond {
			log.Printf("[Coordinator] Task %d timeout\n", t.TaskId)
			t.Status = TaskStatusCreated
			c.taskQueue <- t
		}
	}
}

func (c *Coordinator) createMapTasks(files []string) {
	for i := range files {
		task := &Task{
			TaskId:   c.maxTaskId,
			MapId:    i,
			NReduce:  c.nReduce,
			NFiles:   c.nFiles,
			FileName: files[i],
			Status:   TaskStatusCreated,
		}
		c.maxTaskId++

		c.taskQueue <- task
		c.allTasks[task.TaskId] = task
	}
}

func (c *Coordinator) createReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		task := &Task{
			TaskId:   c.maxTaskId,
			ReduceId: i,
			NFiles:   c.nFiles,
			NReduce:  c.nReduce,
			Status:   TaskStatusCreated,
		}
		c.maxTaskId++

		c.taskQueue <- task
		c.allTasks[task.TaskId] = task
	}
}
