package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type InfoArgs struct {
}

type InfoReply struct {
	NumMapTask    int
	NumReduceTask int
	WorkerId      int
}

type ReportArgs struct {
	ReduceIsEnd bool
	TaskType    string
	WorkerId    int
}

type ReportReply struct {
	KeepWorking bool
	ShouldExit  bool
}

type FinishArgs struct {
	TaskId   int
	WorkerId int
	TaskDone bool
	TaskType string
}

type FinishReply struct {
	OK bool
}

type TaskArgs struct {
	WorkerId int
	TaskType string
}

type TaskReply struct {
	GotTask    bool
	Task       Task
	NumWorker  int
	NumMapTask int
}

type AskReduceArgs struct {
	WorkerId int
}

type AskReduceReply struct {
	CanReduce  bool
	ShouldExit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
