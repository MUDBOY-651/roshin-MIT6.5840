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

var reduceWg sync.WaitGroup

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	const numWorker = 4
	reply := InfoCall(numWorker)
	numReducer := reply.numReducer
	reduceWg.Add(numReducer)

	mapFunc := func(id int) {
		reply := Call(id, "map")
		task := reply.task
		f, err := os.Open(task.fileName)
		if err != nil {
			log.Fatalf("cannot open %v", reply.fileName)
		}
		content, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", reply.fileName)
		}
		if err := f.Close(); err != nil {
			return
		}
		var encoderList []*json.Encoder
		for i := 0; i < numReducer; i++ {
			tempFileName := fmt.Sprintf("mr-%v-%v", id, i)
			f, _ := os.Create(tempFileName)
			encoderList = append(encoderList, json.NewEncoder(f))
		}
		res := mapf(reply.fileName, string(content))
		for _, kv := range res {
			index := ihash(kv.Key)
			if err := encoderList[index].Encode(&kv); err != nil {
				log.Fatalf("cannot encode %v workerId=%v kv={%v}", reply.fileName, id, kv)
			}
		}
	}
	reduceFunc := func(workerId int) {
	}

	for i := 0; i < numWorker; i++ {
		go mapFunc(i)
	}
	// map finished
}

// Call
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func Call(id int, taskType string) Reply {
	args := Args{id, taskType}
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
	fmt.Printf("id:%d reply: %v\n", id, reply)
	return reply
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
