package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	askWork   bool = false
	askEnd    bool = true
	numWorker int  = 4
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type Keys []KeyValue

// for sorting by key.
func (a Keys) Len() int           { return len(a) }
func (a Keys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Keys) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	if _, err := h.Write([]byte(key)); err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
/*
	1. Xth reducer output mr-out-X

tips:
	1. ask for a Task
	2.
*/

var Wg sync.WaitGroup

// Node instance represents a worker
type Node struct {
	workerId      int
	numReduceTask int
	TaskId        int // -1 means Idle
	Map           *func(string, string) []KeyValue
	Reduce        *func(string, []string) string
}

// Call
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func (node *Node) Call(taskType string) Reply {
	args := Args{node.workerId, taskType}
	// declare a reply structure.
	reply := Reply{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.TaskHandler", &args, &reply)
	if ok == false {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	fmt.Printf("id:%d reply: %v\n", node.workerId, reply)
	return reply
}

func (node *Node) mapFunc() bool {
	id := node.workerId
	reply := node.Call("map")
	if reply.GotTask == false {
		return false
	}
	// 应该是一个浅拷贝
	task := reply.Task
	node.TaskId = task.TaskId
	defer func() {
		node.TaskId = -1
	}()
	f, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
		return false
	}
	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
		return false
	}
	if err := f.Close(); err != nil {
		log.Fatalf("Close %v FAILED !", task.FileName)
		return false
	}
	task.TaskState = InProgress
	var encoderList []*json.Encoder
	for i := 0; i < node.numReduceTask; i++ {
		tempFileName := fmt.Sprintf("mr-%v-%v", id, i)
		f, _ := os.Create(tempFileName)
		encoderList = append(encoderList, json.NewEncoder(f))
	}
	res := (*node.Map)(task.FileName, string(content))
	for _, kv := range res {
		index := ihash(kv.Key) % node.numReduceTask
		if err := encoderList[index].Encode(&kv); err != nil {
			FinishCall(task.TaskId, node.workerId, false, "map")
			log.Fatalf("cannot encode %v WorkerId=%v kv={%v}", task.FileName, id, kv)
			return false
		}
	}

	// 发送一个 RPC 请求，告知 master 这个任务已完成
	for reply := FinishCall(task.TaskId, node.workerId, true, "map"); reply.OK == false; {
	}
	Wg.Done()
	return true
}

func (node *Node) reduceFunc() bool {
	// ADD A RPC
	reply := node.Call("reduce")
	task := reply.Task
	if reply.GotTask == false {
		return false
	}
	id := node.workerId
	intermediate := []KeyValue{}
	// 处理一列
	for i := 0; i < numWorker; i++ {
		tempFileName := fmt.Sprintf("mr-%v-%v", i, id)
		f, _ := os.Open(tempFileName)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}
	outputFileName := fmt.Sprintf("mr-out-%d", id)
	ofile, _ := os.Create(outputFileName)
	defer ofile.Close()

	sort.Sort(Keys(intermediate))
	// Need Implementation
	// 合并相同 key
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := (*node.Reduce)(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("Reducer %d Fprintf Error!\n", id)
		}
		i = j
	}

	for reply := FinishCall(task.TaskId, node.workerId, true, "reduce"); reply.OK == false; {
	}
	// Need Implementation
	Wg.Done()
	return true
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := InfoCall(numWorker)
	workers := make([]*Node, numWorker)
	for i := 0; i < numWorker; i++ {
		workers[i] = new(Node)
		workers[i].TaskId = -1
		workers[i].workerId = i
		workers[i].numReduceTask = reply.NumReduceTask
		workers[i].Map = &mapf
		workers[i].Reduce = &reducef
	}
	numReduceTask, numMapTask := reply.NumReduceTask, reply.NumMapTask

	Wg.Add(numMapTask)
	for reply := ReportCall(askWork, "map"); reply.KeepWorking == true; {
		for i := 0; i < numWorker; i++ {
			// TaskId == -1 means the worker is Idle
			if workers[i].TaskId == -1 {
				go workers[i].mapFunc()
			}
		}
	}
	Wg.Wait()
	// REDUCE
	Wg.Add(numReduceTask)
	for reply := ReportCall(askWork, "reduce"); reply.KeepWorking == true; {
		for i := 0; i < numWorker; i++ {
			if workers[i].TaskId == -1 {
				go workers[i].reduceFunc()
			}
		}
	}
	Wg.Wait()
	ReportCall(askEnd, "")
}

func InfoCall(numWorker int) InfoReply {
	args := InfoArgs{numWorker}
	reply := InfoReply{}
	ok := call("Coordinator.InfoHandler", &args, &reply)
	if ok == false {
		fmt.Printf("InfoCall failed!\n")
		os.Exit(0)
	}
	return reply
}

func ReportCall(askEnd bool, taskType string) ReportReply {
	args := ReportArgs{askEnd, taskType}
	reply := ReportReply{}
	ok := call("Coordinator.ReportHandler", &args, &reply)
	if ok == false {
		fmt.Printf("ReportCall failed!\n")
	}
	return reply
}

func FinishCall(taskId int, workerId int, taskDone bool, taskType string) FinishReply {
	// Need Implementation
	args := FinishArgs{taskId, workerId, taskDone, taskType}
	reply := FinishReply{}
	ok := call("Coordinator.FinishHandler", &args, &reply)
	if ok == false {
		fmt.Printf("FinishCall failed!\n")
	}
	return reply
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
	defer func(c *rpc.Client) {
		if err := c.Close(); err != nil {
		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
