package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type AWorker struct {
	// 当前任务
	task    *Task
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	status  int
	ticker  *time.Ticker
}

// handle map
func (w *AWorker) handleMap() {
	// read from file
	fileName := w.task.FileName
	content, err := readFile(fileName)
	if err != nil {
		log.Fatalf("<handleMap> Failed to read file: %s", fileName)
	}

	// do map
	kva := w.mapf(w.task.FileName, string(content))

	// write intermediate to file
	writeIntermediate(kva, w.task.MapId, w.task.NReduce)
}

func writeIntermediate(kva []KeyValue, mapId int, nReduce int) {
	intermediate := make(map[int][]KeyValue)
	for idx := range kva {
		i := ihash(kva[idx].Key) % nReduce // reduceId
		intermediate[i] = append(intermediate[i], kva[idx])
	}

	writeToFile := func(k int) {
		oname := fmt.Sprintf("mr-%d-%d", mapId, k)
		ofile, err := os.Create(oname)
		defer ofile.Close()
		if err != nil {
			log.Fatalf("Failed to create file: %s", oname)
			return
		}
		bytes, err := json.Marshal(intermediate[k])
		if err != nil {
			log.Fatalf("Failed to marshal value")
			return
		}
		fmt.Fprintf(ofile, "%v\n", string(bytes))
	}

	for k := range intermediate {
		writeToFile(k)
	}
}

// handle reduce
func (w *AWorker) handleReduce() {
	// read intermediate from file
	allKva := w.readIntermediate()

	// sort
	sort.Sort(ByKey(allKva))

	// do reduce and write to file
	reduceId := w.task.ReduceId
	oname := "mr-out-" + strconv.Itoa(reduceId)
	ofile, err := os.Create(oname)
	defer ofile.Close()
	if err != nil {
		log.Fatalf("Failed to create file: %s", oname)
	}

	for i := 0; i < len(allKva); {
		j := i + 1
		for j < len(allKva) && allKva[j].Key == allKva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allKva[k].Value)
		}
		output := w.reducef(allKva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", allKva[i].Key, output)
		i = j
	}
}

func (w *AWorker) readIntermediate() (allKva []KeyValue) {
	// read intermediate
	reduceId := w.task.ReduceId
	allKva = make([]KeyValue, 0)
	for i := 0; i < w.task.NFiles; i++ {
		ifile := fmt.Sprintf("mr-%d-%d", i, reduceId)
		content, err := readFile(ifile)
		if err != nil {
			continue
		}
		kva := []KeyValue{}
		err = json.Unmarshal([]byte(content), &kva)
		if err != nil {
			log.Printf("[Worker] Failed to unmarshal json: %s", content)
			continue
		}
		allKva = append(allKva, kva...)
	}
	return
}

func (w *AWorker) process() {
	for {
		args := &RequestForTaskArgs{}
		reply := &RequestForTaskReply{}
		if w.task != nil {
			// 任务完成，提交任务
			args.TaskId = w.task.TaskId
			args.Status = TaskStatusCompleted
			log.Printf("[Worker] complete task %d", args.TaskId)
		}
		if !call("Coordinator.RequestForTask", args, reply) {
			log.Println("Call Coordinator.RequestForTask Error")
			break
		}
		if reply.Done {
			break
		}

		w.task = &reply.Task

		switch reply.Ttype {
		case TaskTypeMap:
			log.Printf("[Worker] Receive map task id: %d", w.task.TaskId)
			w.startTicker()
			w.handleMap()
			w.ticker.Stop()
		case TaskTypeReduce:
			log.Printf("[Worker] Receive reduce task id: %d", w.task.TaskId)
			w.startTicker()
			w.handleReduce()
			w.ticker.Stop()
		case TaskTypeNone:
			log.Printf("[Worker] Receive type none")
			time.Sleep(time.Second)
		default:
			log.Printf("[Worker] Unknown task type: %d\n", reply.Ttype)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := &AWorker{
		mapf:    mapf,
		reducef: reducef,
		ticker:  time.NewTicker(time.Second),
	}
	worker.process()
	worker.ticker.Stop()
}

// 开始发送心跳的ticker
func (w *AWorker) startTicker() {
	go func() {
		for {
			select {
			case <-w.ticker.C:
				w.heartbeat()
			}
		}
	}()
}

func (w *AWorker) heartbeat() {
	args := &PingArgs{
		TaskId: w.task.TaskId,
	}

	reply := &PingReply{}
	call("Coordinator.Ping", &args, &reply)
}

func readFile(fileName string) (string, error) {
	file, err := os.Open(fileName)
	defer file.Close()

	if err != nil {
		return "", errors.New("cannot open " + fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", errors.New("cannot read " + fileName)
	}
	return string(content), nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
