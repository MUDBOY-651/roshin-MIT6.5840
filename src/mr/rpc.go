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
	numReducer int
}

type Args struct {
	workerId int
	taskType string
}

type Reply struct {
	task     Task
	workerId int
	fileName string
	rspType  int // 0 Map 1 Reduce 2 ?
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
