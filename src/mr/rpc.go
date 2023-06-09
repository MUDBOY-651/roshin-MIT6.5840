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
	numWorker int
}

type InfoReply struct {
	numMapTask    int
	numReduceTask int
}

type ReportArgs struct {
	reduceIsEnd bool
	taskType    string
}

type ReportReply struct {
	keepWorking bool
}

type FinishArgs struct {
	taskId   int
	workerId int
	taskDone bool
	taskType string
}

type FinishReply struct {
	OK bool
}

type Args struct {
	workerId int
	taskType string
}

type Reply struct {
	gotTask bool
	task    Task
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
