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
	NumWorker int
}

type InfoReply struct {
	NumMapTask    int
	NumReduceTask int
}

type ReportArgs struct {
	ReduceIsEnd bool
	TaskType    string
}

type ReportReply struct {
	KeepWorking bool
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

type Args struct {
	WorkerId int
	TaskType string
}

type Reply struct {
	GotTask bool
	Task    Task
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
