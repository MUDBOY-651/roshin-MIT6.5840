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
	FileName  string
	TaskState int8
	WorkerId  int
	TaskId    int
}

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

func (c *Coordinator) TaskHandler(args *Args, reply *Reply) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if (args.TaskType == "map" && c.mapTaskList.Len() == 0) ||
		(args.TaskType == "reduce" && c.reduceTaskList.Len() == 0) {
		reply.GotTask = false
		return nil
	}
	if args.TaskType == "map" {
		taskId, _ := c.mapTaskList.Back().Value.(int)
		c.mapTaskList.Remove(c.mapTaskList.Back())
		reply.Task = c.mapTask[taskId]
	} else {
		taskId, _ := c.reduceTaskList.Back().Value.(int)
		c.reduceTaskList.Remove(c.reduceTaskList.Back())
		reply.Task = c.reduceTask[taskId]
	}
	// Need Implementation
	reply.GotTask = true
	return nil
}

func (c *Coordinator) InfoHandler(args *InfoArgs, reply *InfoReply) error {
	c.numWorker = args.NumWorker
	reply.NumMapTask = c.numMapTask
	reply.NumReduceTask = c.numReduceTask
	return nil
}

func (c *Coordinator) ReportHandler(args *ReportArgs, reply *ReportReply) error {
	if args.ReduceIsEnd == true {
		c.Over = true
	} else if args.TaskType == "map" && c.numDoneMapTask != c.numMapTask {
		reply.KeepWorking = true
	} else if args.TaskType == "reduce" && c.numDoneReduceTask != c.numReduceTask {
		reply.KeepWorking = true
	}
	return nil
}

func (c *Coordinator) FinishHandler(args *FinishArgs, reply *FinishReply) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	var task Task
	if args.TaskType == "map" {
		task = c.mapTask[args.TaskId]
	} else if args.TaskType == "reduce" {
		task = c.reduceTask[args.TaskId]
	}
	if args.TaskDone == false {
		task.TaskState = Idle
		reply.OK = true
		if args.TaskType == "map" {
			c.mapTaskList.PushBack(task.TaskId)
		} else if args.TaskType == "reduce" {
			c.reduceTaskList.PushBack(task.TaskId)
		}
		return nil
	}
	if args.TaskType == "map" {
		c.numDoneMapTask++
	} else if args.TaskType == "reduce" {
		c.numDoneReduceTask++
	}
	// Need Implementation
	reply.OK = true
	task.TaskState = Completed
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
	c.mapTaskList = new(list.List)
	c.reduceTaskList = new(list.List)
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
