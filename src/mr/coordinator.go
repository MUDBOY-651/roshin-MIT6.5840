package mr

import (
	"container/list"
	"log"
	"math"
	"sync"
	"sync/atomic"
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
	id2TaskId         map[int]int
	Over              atomic.Value
}

// IsOutDate t1 < t2，检查有没有超过 10 秒钟没响应
func IsOutDate(now time.Time, prev time.Time) bool {
	sub := now.Sub(prev)
	return math.Abs(sub.Seconds()) >= 10.0
}

type Val struct {
	workerId int
	TTL      time.Time
}

func (c *Coordinator) SetActive(workerId int) {
	c.TaskLock(workerId)
	defer c.TaskUnlock(workerId)
	if temp, ok := c.id2ListNode[workerId]; ok {
		c.lastActiveList.Remove(temp)
		delete(c.id2ListNode, workerId)
    delete(c.id2TaskId, workerId)
	}
	elem := Val{workerId, time.Now()}
	c.lastActiveList.PushBack(elem)
	c.id2ListNode[workerId] = c.lastActiveList.Back()
}

func (c *Coordinator) CheckAlive(workerId int) bool {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if _, ok := c.id2TaskId[workerId]; !ok {
		return false
	}
	return true
}

func (c *Coordinator) TaskLock(workerId int) {
	log.Printf("Worker#%d Try taskLock", workerId)
	c.taskLock.Lock()
	log.Printf("Worker#%d taskLock Lock!", workerId)
}

func (c *Coordinator) TaskUnlock(workerId int) {
	log.Printf("Worker#%d Unlock!", workerId)
	c.taskLock.Unlock()
}

func (c *Coordinator) TaskHandler(args *TaskArgs, reply *TaskReply) error {
	c.SetActive(args.WorkerId)
	log.Printf("[TaskHandler] Handle Worker#%d %s Task", args.WorkerId, args.TaskType)
	c.TaskLock(args.WorkerId)
	defer func() {
		c.TaskUnlock(args.WorkerId)
	}()
	if (args.TaskType == "map" && c.mapTaskList.Len() == 0 && !c.CheckWorker()) ||
		(args.TaskType == "reduce" && c.reduceTaskList.Len() == 0 && !c.CheckWorker()) {
		reply.GotTask = false
		return nil
	}
  if args.TaskType == "map" && c.mapTaskList.Len() == 0 && c.reduceTaskList.Len() != c.numReduceTask {
    reply.GotTask = false
    return nil
  }
	reply.GotTask = true
	reply.NumMapTask = c.numMapTask
	reply.NumWorker = c.numWorker
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
	log.Printf("[ReportHandler] Worker#%d requests, state: {%d/%d, %d/%d} \n",
		args.WorkerId, c.numDoneMapTask, c.numMapTask, c.numDoneReduceTask, c.numReduceTask)
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if args.ReduceIsEnd == true {
		c.Over.Store(true)
	} else if args.TaskType == "map" && c.numDoneMapTask < c.numMapTask {
		reply.KeepWorking = true
	} else if args.TaskType == "reduce" && c.numDoneReduceTask < c.numReduceTask {
		reply.KeepWorking = true
	}
	return nil
}

func (c *Coordinator) FinishHandler(args *FinishArgs, reply *FinishReply) error {
	if c.CheckAlive(args.WorkerId) == false {
		reply.OK = true
		return nil
	}
	c.SetActive(args.WorkerId)
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	var task *Task
	if args.TaskType == "map" {
		task = &c.mapTask[args.TaskId]
	} else if args.TaskType == "reduce" {
		task = &c.reduceTask[args.TaskId]
	}
	delete(c.id2TaskId, args.WorkerId)
  temp := c.id2ListNode[args.WorkerId]
  c.lastActiveList.Remove(temp)
  delete(c.id2ListNode, args.WorkerId)
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
	if task.TaskState == Completed {
		reply.OK = true
		return nil
	}
	if args.TaskType == "map" {
		c.numDoneMapTask++
	} else if args.TaskType == "reduce" {
		c.numDoneReduceTask++
	}
	// Need Implementation
  log.Printf("[FinishHandler] Worker#%d completed %sTask-%d {%d/%d, %d/%d}\n", args.WorkerId, args.TaskType, args.TaskId,
              c.numDoneMapTask, c.numMapTask, c.numDoneReduceTask, c.numReduceTask)
	reply.OK = true
	task.TaskState = Completed
	return nil
}

func (c *Coordinator) AskReduceHandler(args *AskReduceArgs, reply *AskReduceReply) error {
	c.SetActive(args.WorkerId)
	if c.numDoneMapTask >= c.numMapTask {
		reply.CanReduce = true
	} else {
		reply.CanReduce = false
	}
	return nil
}

func (c *Coordinator) KickWorker(workerId int) {
	taskId := c.id2TaskId[workerId]
	delete(c.id2TaskId, workerId)
  temp := c.id2ListNode[workerId]
  c.lastActiveList.Remove(temp)
	if c.numDoneMapTask >= c.numMapTask {
		c.reduceTask[taskId].TaskState = Idle
		c.reduceTaskList.PushBack(taskId)
	} else {
		c.mapTask[taskId].TaskState = Idle
		c.mapTaskList.PushBack(taskId)
	}
	log.Printf("[KickWorker] Worker#%d, Kicked, ", workerId)
	log.Printf("Lmap=%d Lreduce=%d\n", c.mapTaskList.Len(), c.reduceTaskList.Len())
}

func (c *Coordinator) CheckWorker() bool {
	now := time.Now()
	if c.lastActiveList.Len() == 0 {
		return false
	}
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
	ret := c.Over.Load().(bool)
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
	c.Over.Store(false)
	for id, fileName := range files {
		c.mapTask = append(c.mapTask, Task{fileName, Idle, -1, id})
		c.mapTaskList.PushBack(id)
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskList.PushBack(i)
		var task Task
		task.TaskId = i
		c.reduceTask = append(c.reduceTask, task)
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
	go func() {
		for c.Over.Load().(bool) != true {
			c.taskLock.Lock()
			c.CheckWorker()
			c.taskLock.Unlock()
			time.Sleep(8 * time.Second)
		}
	}()
	go http.Serve(l, nil)
}
