package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

const numWorker = 4

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
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
	1. ask for a task
	2.
*/

var Wg sync.WaitGroup

type Node struct {
	workerId      int
	numReduceTask int
	taskId        int // -1 means Idle
	Map           func(string, string) []KeyValue
	Reduce        func(string, []string) string
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
	ok := call("Coordinator.taskHandler", &args, &reply)
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
	// 应该是一个浅拷贝
	task := reply.task
	f, err := os.Open(task.fileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.fileName)
		return false
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", task.fileName)
		return false
	}
	if err := f.Close(); err != nil {
		log.Fatalf("Close %v FAILED !", task.fileName)
		return false
	}

	task.taskState = InProgress
	var encoderList []*json.Encoder
	for i := 0; i < node.numReduceTask; i++ {
		tempFileName := fmt.Sprintf("mr-%v-%v", id, i)
		f, _ := os.Create(tempFileName)
		encoderList = append(encoderList, json.NewEncoder(f))
	}
	res := node.Map(task.fileName, string(content))
	for _, kv := range res {
		index := ihash(kv.Key)
		if err := encoderList[index].Encode(&kv); err != nil {
			log.Fatalf("cannot encode %v workerId=%v kv={%v}", task.fileName, id, kv)
		}
	}

	// 发送一个 RPC 请求，告知 master 这个任务已完成
	FinishCall(task.taskId, node.workerId)
	Wg.Done()
	return true
}

func (node *Node) reduceFunc() bool {
	// Need Implementation

	Wg.Done()
	return true
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	reply := InfoCall(numWorker)
	workers := [numWorker]*Node{new(Node)}
	for id, node := range workers {
		node.taskId = -1
		node.workerId = id
		node.Map = mapf
		node.Reduce = reducef
	}
	numReduceTask, numMapTask := reply.numReduceTask, reply.numMapTask
	const (
		askWork bool = false
		askEnd  bool = true
	)
	Wg.Add(numMapTask)
	for reply := ReportCall(askWork); reply.keepWorking == true; {
		for i := 0; i < numWorker; i++ {
			if workers[i].taskId == -1 {
				go workers[i].mapFunc()
			}
		}
	}
	Wg.Wait()
	// map finished
	Wg.Add(numReduceTask)
	for reply := ReportCall(askWork); reply.keepWorking == true; {
		for i := 0; i < numWorker; i++ {
			if workers[i].taskId == -1 {
				go workers[i].reduceFunc()
			}
		}
	}
	Wg.Wait()
	ReportCall(askEnd)
}

func InfoCall(numWorker int) InfoReply {
	args := InfoArgs{numWorker}
	reply := InfoReply{}
	ok := call("Coordinator.infoHandler", &args, &reply)
	if ok == false {
		fmt.Printf("InfoCall failed!\n")
		os.Exit(0)
	}
	return reply
}

func ReportCall(askEnd bool) ReportReply {
	args := ReportArgs{}
	reply := ReportReply{}
	args.reduceIsEnd = askEnd
	ok := call("Coordinator.reportHandler", &args, &reply)
	if ok == false {
		fmt.Printf("ReportCall failed!\n")
	}
	return reply
}

func FinishCall(taskId int, workerId int) ReportReply {
	// Need Implementation
	args := FinishArgs{taskId, workerId}
	reply := ReportReply{}

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
