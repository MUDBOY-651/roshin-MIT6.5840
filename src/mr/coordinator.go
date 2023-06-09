package mr

import (
	"container/list"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Idle       int8 = 0
	InProgress int8 = 1
	Completed  int8 = 2
)

type Task struct {
	fileName  string
	taskState int8
	workerId  int
	taskId    int
}

type void struct{}

var NULL void

type Coordinator struct {
	// mapTask != numWorker
	mapTask           []Task
	reduceTask        []Task
	mapTaskList       *list.List
	reduceTaskList    *list.List
	numWorker         int
	numReduceTask     int
	numMapTask        int
	numDoneMapTask    int
	numDoneReduceTask int
	taskLock          sync.Mutex
	Over              bool
}

// Your code here -- RPC handlers for the worker to call.

// Handle Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) taskHandler(args *Args, reply *Reply) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if (args.taskType == "map" && c.mapTaskList.Len() == 0) ||
		(args.taskType == "reduce" && c.reduceTaskList.Len() == 0) {
		reply.gotTask = false
		return nil
	}
	if args.taskType == "map" {
		taskId, _ := c.mapTaskList.Back().Value.(int)
		c.mapTaskList.Remove(c.mapTaskList.Back())
		reply.task = c.mapTask[taskId]
	} else {
		taskId, _ := c.reduceTaskList.Back().Value.(int)
		c.reduceTaskList.Remove(c.reduceTaskList.Back())
		reply.task = c.reduceTask[taskId]
	}
	// Need Implementation
	reply.gotTask = true
	return nil
}

func (c *Coordinator) infoHandler(args *InfoArgs, reply *InfoReply) error {
	c.numWorker = args.numWorker
	reply.numMapTask = c.numMapTask
	reply.numReduceTask = c.numReduceTask
	return nil
}

func (c *Coordinator) reportHandler(args *ReportArgs, reply *ReportReply) error {
	if args.reduceIsEnd == true {
		c.Over = true
	} else if args.taskType == "map" && c.numDoneMapTask != c.numMapTask {
		reply.keepWorking = true
	} else if args.taskType == "reduce" && c.numDoneReduceTask != c.numReduceTask {
		reply.keepWorking = true
	}
	return nil
}

func (c *Coordinator) finishHandler(args *FinishArgs, reply *FinishReply) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	var task Task
	if args.taskType == "map" {
		task = c.mapTask[args.taskId]
	} else {
		task = c.reduceTask[args.taskId]
	}
	if args.taskDone == false {
		task.taskState = Idle
		reply.OK = true
		if args.taskType == "map" {
			c.mapTaskList.PushBack(task.taskId)
		} else {
			c.reduceTaskList.PushBack(task.taskId)
		}
		return nil
	}
	if args.taskType == "map" {
		c.numDoneMapTask++
	} else {
		c.numReduceTask++
	}
	// Need Implementation
	reply.OK = true
	task.taskState = Completed
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.Over
	// Your code here.
	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.numReduceTask = nReduce
	for id, fileName := range files {
		c.mapTask = append(c.mapTask, Task{fileName, Idle, -1, id})
		c.mapTaskList.PushBack(id)
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskList.PushBack(i)
	}
	c.numMapTask = len(c.mapTask)

	c.server()
	return &c
}
