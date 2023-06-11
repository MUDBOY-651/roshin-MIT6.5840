package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
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

// use ihash(key) % NReduce to choose to reduce
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

// Node instance represents a worker
type Node struct {
	WorkerId      int
	NumReduceTask int
	TaskId        int // -1 means Idle
	Map           *func(string, string) []KeyValue
	Reduce        *func(string, []string) string
}

func NewWorker(workerId int, numReduceTask int, taskId int, mapf *func(string, string) []KeyValue, reducef *func(string, []string) string) *Node {
	node := Node{}
	node.WorkerId = workerId
	node.NumReduceTask = numReduceTask
	node.TaskId = taskId
	node.Map = mapf
	node.Reduce = reducef
	return &node
}

// Call
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func (node *Node) Call(taskType string) TaskReply {
	args := TaskArgs{node.WorkerId, taskType}
	// declare a reply structure.
	reply := TaskReply{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.TaskHandler", &args, &reply)
	if ok == false {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	fmt.Printf("id:%d reply: %v\n", node.WorkerId, reply)
	return reply
}

func (node *Node) mapFunc() bool {
	id := node.WorkerId
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
	var encoderList []*json.Encoder
	for i := 0; i < node.NumReduceTask; i++ {
		tempFileName := fmt.Sprintf("mr-%v-%v", id, i)
		f, _ := os.Create(tempFileName)
		encoderList = append(encoderList, json.NewEncoder(f))
	}
	res := (*node.Map)(task.FileName, string(content))
	for _, kv := range res {
		index := ihash(kv.Key) % node.NumReduceTask
		if err := encoderList[index].Encode(&kv); err != nil {
			node.FinishCall(false, "map")
			log.Fatalf("cannot encode %v WorkerId=%v kv={%v}", task.FileName, id, kv)
			return false
		}
	}

	// 发送一个 RPC 请求，告知 master 这个任务已完成
	for reply := node.FinishCall(true, "map"); reply.OK == false; {
	}
	return true
}

func (node *Node) reduceFunc() bool {
	// ADD A RPC
	reply := node.Call("reduce")
	task := reply.Task
	node.TaskId = task.TaskId
	if reply.GotTask == false {
		return false
	}
	id := node.WorkerId
	intermediate := []KeyValue{}
	// 处理一列
	for i := 0; i < reply.NumWorker; i++ {
		tempFileName := fmt.Sprintf("mr-%v-%v", i, node.TaskId)
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

	for reply := node.FinishCall(true, "reduce"); reply.OK == false; {
	}
	// Need Implementation
	return true
}

func (node *Node) AskReduceCall() AskReduceReply {
	args := AskReduceArgs{node.WorkerId}
	reply := AskReduceReply{}
	ok := call("Coordinator.AskReduceHandler", &args, &reply)
	if ok == false {
		fmt.Printf("ReportCall failed!\n")
	}
	return reply
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := InfoCall()
	worker := NewWorker(reply.WorkerId, reply.NumReduceTask, -1, &mapf, &reducef)

	for reply := worker.ReportCall(askWork, "map"); reply.KeepWorking == true; time.Sleep(time.Second) {
		// TaskId == -1 means the worker is Idle
		worker.mapFunc()
	}
	for reply := worker.AskReduceCall(); reply.CanReduce == false; time.Sleep(time.Second) {
		log.Printf("Worker %d waiting for Map Finished\n", worker.WorkerId)
	}
	// REDUCE
	for reply := worker.ReportCall(askWork, "reduce"); reply.KeepWorking == true; time.Sleep(time.Second) {
		worker.reduceFunc()
	}
	worker.ReportCall(askEnd, "")
}

func InfoCall() InfoReply {
	args := InfoArgs{}
	reply := InfoReply{}
	ok := call("Coordinator.InfoHandler", &args, &reply)
	if ok == false {
		fmt.Printf("InfoCall failed!\n")
		os.Exit(0)
	}
	return reply
}

func (node *Node) ReportCall(askEnd bool, taskType string) ReportReply {
	args := ReportArgs{askEnd, taskType, node.WorkerId}
	reply := ReportReply{}
	ok := call("Coordinator.ReportHandler", &args, &reply)
	if ok == false {
		fmt.Printf("ReportCall failed!\n")
	}
	return reply
}

func (node *Node) FinishCall(taskDone bool, taskType string) FinishReply {
	// Need Implementation
	args := FinishArgs{node.TaskId, node.WorkerId, taskDone, taskType}
	reply := FinishReply{}
	ok := call("Coordinator.FinishHandler", &args, &reply)
	if ok == false {
		fmt.Printf("FinishCall failed!\n")
	}
	//
	node.TaskId = -1
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
