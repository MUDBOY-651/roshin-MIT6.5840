package mr

import (
	"container/list"
	"log"
	"math"
	"sync"
	"time"
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
	infoLock          sync.Mutex
	id2ListNode       map[int]*list.Element
	lastActiveList    *list.List // List<time.TIme>
	listLock          sync.RWMutex
	id2TaskId         map[int]int
	Over              bool
}

// IsOutDate t1 < t2，检查有没有超过 10 秒钟没响应
func IsOutDate(now time.Time, prev time.Time) bool {
	sub := now.Sub(prev)
	return math.Abs(sub.Seconds()) > 10.0
}

type Val struct {
	workerId int
	TTL      time.Time
}

func (c *Coordinator) SetActive(workerId int) {
	c.listLock.Lock()
	defer c.listLock.Unlock()
	if temp, ok := c.id2ListNode[workerId]; ok {
		c.lastActiveList.Remove(temp)
		delete(c.id2ListNode, workerId)
	}
	elem := Val{workerId, time.Now()}
	c.lastActiveList.PushBack(elem)
	c.id2ListNode[workerId] = c.lastActiveList.Back()
}

func (c *Coordinator) TaskHandler(args *TaskArgs, reply *TaskReply) error {
	c.SetActive(args.WorkerId)
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if (args.TaskType == "map" && c.mapTaskList.Len() == 0 && !c.CheckWorker()) ||
		(args.TaskType == "reduce" && c.reduceTaskList.Len() == 0 && !c.CheckWorker()) {
		reply.GotTask = false
		return nil
	}
	reply.GotTask = true
	if args.TaskType == "map" {
		taskId, _ := c.mapTaskList.Back().Value.(int)
		c.mapTaskList.Remove(c.mapTaskList.Back())
		reply.Task = c.mapTask[taskId]
		c.mapTask[taskId].TaskState = InProgress
		c.id2TaskId[args.WorkerId] = taskId
	} else {
		taskId, _ := c.reduceTaskList.Back().Value.(int)
		c.reduceTaskList.Remove(c.reduceTaskList.Back())
		reply.Task = c.reduceTask[taskId]
		c.reduceTask[taskId].TaskState = InProgress
		c.id2TaskId[args.WorkerId] = taskId
		reply.NumWorker = c.numWorker
	}
	// Need Implementation
	return nil
}

func (c *Coordinator) InfoHandler(args *InfoArgs, reply *InfoReply) error {
	c.infoLock.Lock()
	defer c.infoLock.Unlock()
	reply.WorkerId = c.numWorker
	c.numWorker++
	reply.NumMapTask = c.numMapTask
	reply.NumReduceTask = c.numReduceTask
	c.SetActive(reply.WorkerId)
	return nil
}

func (c *Coordinator) ReportHandler(args *ReportArgs, reply *ReportReply) error {
	c.SetActive(args.WorkerId)
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
	c.SetActive(args.WorkerId)
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	var task *Task
	if args.TaskType == "map" {
		task = &c.mapTask[args.TaskId]
	} else if args.TaskType == "reduce" {
		task = &c.reduceTask[args.TaskId]
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
	delete(c.id2TaskId, args.WorkerId)
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

func (c *Coordinator) AskReduceHandler(args *AskReduceArgs, reply *AskReduceReply) error {
	c.SetActive(args.WorkerId)
	reply.CanReduce = c.numDoneMapTask == c.numReduceTask
	return nil
}

func (c *Coordinator) KickWorker(workerId int) {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	taskId := c.id2TaskId[workerId]
	if c.numDoneMapTask == 0 {
		c.reduceTask[taskId].TaskState = Idle
		c.reduceTaskList.PushBack(taskId)
	} else {
		c.mapTask[taskId].TaskState = Idle
		c.mapTaskList.PushBack(taskId)
	}
}

func (c *Coordinator) CheckWorker() bool {
	now := time.Now()
	head := c.lastActiveList.Front()
	val := head.Value.(Val)
	if IsOutDate(now, val.TTL) {
		c.KickWorker(val.workerId)
		return true
	}
	return false
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
	c.numWorker = 0
	c.numReduceTask = nReduce
	c.mapTaskList = new(list.List)
	c.reduceTaskList = new(list.List)
	c.lastActiveList = new(list.List)
	c.id2TaskId = make(map[int]int)
	c.id2ListNode = make(map[int]*list.Element)
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
