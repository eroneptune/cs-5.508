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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := RequestReply{}
	registerSuccess := call("Coordinator.Register", &RequestArgs{}, &reply)
	if !registerSuccess {
		fmt.Printf("register failed!\n")
	}

	worker := WorkerInfo{reply.Worker.WorkerId, IDLE_WORKER, INVALID_TASK_ID, time.Now().Unix()}
	args := RequestArgs{worker.WorkerId, IDLE_WORKER}

	for {
		reply := RequestReply{}
		ok := call("Coordinator.Call", &args, &reply)
		if !ok {
			break
		} else {
			if reply.NReduce == -1 {

				// sleep 500ms
				time.Sleep(500 * time.Millisecond)
			} else if reply.Worker.Status == MAPPING_WORKER {
				// map
				content, err := readFile(reply.Task.FileName)
				if err != nil {
					// TODO
					return
				}
				kva := mapf(strconv.Itoa(reply.Task.TaskId), content)
				hashIntermediate(kva, reply.Task.TaskId, reply.NReduce)
			} else if reply.Worker.Status == REDUCING_WORKER {
				// reduce
				// reply.Task.reduceId  => reduceId
				// reply.Task.TaskId		=> max taskId
				reduceId := reply.Task.ReduceId
				allKva := make([]KeyValue, 0)
				for i := 0; i < reply.Task.FileSum; i++ {
					content, err := readFile("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId))
					if err != nil {
						// TODO
						fmt.Println("error")
						return
					}
					kva := []KeyValue{}
					err = json.Unmarshal([]byte(content), &kva)
					if err != nil {
						// TODO
						return
					}
					allKva = append(allKva, kva...)
				}
				sort.Sort(ByKey(allKva))
				oname := "mr-out-" + strconv.Itoa(reduceId)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(allKva) {
					j := i + 1
					for j < len(allKva) && allKva[j].Key == allKva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, allKva[k].Value)
					}
					output := reducef(allKva[i].Key, values)
					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", allKva[i].Key, output)
					i = j
				}
				ofile.Close()

			}
		}
	}
}

func hashIntermediate(kva []KeyValue, taskId int, nReduce int) {
	intermediate := make(map[int][]KeyValue)
	for _, val := range kva {
		i := ihash(val.Key) % nReduce // reduceId
		intermediate[i] = append(intermediate[i], val)
	}
	for k, v := range intermediate {
		oname := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(k)
		ofile, _ := os.Create(oname)
		bytes, _ := json.Marshal(v)
		stringData := string(bytes)
		fmt.Fprintf(ofile, "%v\n", stringData)
		ofile.Close()
	}
}

func readFile(fileName string) (string, error) {
	fmt.Println(fileName)
	result := ""
	file, err := os.Open(fileName)
	if err != nil {
		return "", errors.New("cannot open " + fileName)
	}
	content, err := ioutil.ReadAll(file)
	result = result + string(content)
	if err != nil {
		return "", errors.New("cannot read " + fileName)
	}
	file.Close()
	return result, nil
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
