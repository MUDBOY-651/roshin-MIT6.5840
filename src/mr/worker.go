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

const numWorker = 4

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

// Node instance represents a worker
type Node struct {
	workerId      int
	numReduceTask int
	taskId        int // -1 means Idle
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
	if reply.gotTask == false {
		return false
	}
	// 应该是一个浅拷贝
	task := reply.task
	node.taskId = task.taskId
	defer func() {
		node.taskId = -1
	}()
	f, err := os.Open(task.fileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.fileName)
		return false
	}
	content, err := io.ReadAll(f)
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
	res := (*node.Map)(task.fileName, string(content))
	for _, kv := range res {
		index := ihash(kv.Key)
		if err := encoderList[index].Encode(&kv); err != nil {
			FinishCall(task.taskId, node.workerId, false, "map")
			log.Fatalf("cannot encode %v workerId=%v kv={%v}", task.fileName, id, kv)
			return false
		}
	}

	// 发送一个 RPC 请求，告知 master 这个任务已完成
	for reply := FinishCall(task.taskId, node.workerId, true, "map"); reply.OK == false; {
	}
	Wg.Done()
	return true
}

func (node *Node) reduceFunc() bool {
	// ADD A RPC
	reply := node.Call("reduce")
	if reply.gotTask == false {
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
	sort.Sort(Keys(intermediate))
	outputFileName := fmt.Sprintf("mr-out-%d", id)
	ofile, _ := os.Create(outputFileName)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// Need Implementation
	Wg.Done()
	return true
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := InfoCall(numWorker)
	workers := [numWorker]*Node{}
	for id, node := range workers {
		node.taskId = -1
		node.workerId = id
		node.numReduceTask = reply.numReduceTask
		node.Map = &mapf
		node.Reduce = &reducef
	}
	numReduceTask, numMapTask := reply.numReduceTask, reply.numMapTask
	const (
		askWork bool = false
		askEnd  bool = true
	)
	Wg.Add(numMapTask)
	for reply := ReportCall(askWork, "map"); reply.keepWorking == true; {
		for i := 0; i < numWorker; i++ {
			// taskId == -1 means the worker is Idle
			if workers[i].taskId == -1 {
				go workers[i].mapFunc()
			}
		}
	}
	Wg.Wait()
	// map finished
	Wg.Add(numReduceTask)
	for reply := ReportCall(askWork, "reduce"); reply.keepWorking == true; {
		for i := 0; i < numWorker; i++ {
			if workers[i].taskId == -1 {
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
	ok := call("Coordinator.infoHandler", &args, &reply)
	if ok == false {
		fmt.Printf("InfoCall failed!\n")
		os.Exit(0)
	}
	return reply
}

func ReportCall(askEnd bool, taskType string) ReportReply {
	args := ReportArgs{askEnd, taskType}
	reply := ReportReply{}
	ok := call("Coordinator.reportHandler", &args, &reply)
	if ok == false {
		fmt.Printf("ReportCall failed!\n")
	}
	return reply
}

func FinishCall(taskId int, workerId int, taskDone bool, taskType string) FinishReply {
	// Need Implementation
	args := FinishArgs{taskId, workerId, taskDone, taskType}
	reply := FinishReply{}
	ok := call("Coordinator.finishHandler", &args, &reply)
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
