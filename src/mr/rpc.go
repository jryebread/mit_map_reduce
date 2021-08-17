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

const (
	mapTask = iota  // c0 == 0
	reduceTask = iota  // c1 == 1
	waitTask = iota
)

const (
	taskNotStarted = iota
	taskInProgress = iota
	taskCompleted = iota
)

type ExampleArgs struct {
	X int
}

type TaskRequest struct {
}

type TaskReply struct {
	Filename string
	TaskType int 
	Reducers int
	MapJobID int
	ReduceJobID int
	BucketID int
}

type NotifyMapDoneRequest struct {
	MapJobID int //this will map to file on coordinator side
	IntFileNames []string
}

type NotifyMapDoneResponse struct {

}

type NotifyReduceDoneRequest struct {
	ReduceJobID int
}

type NotifyReduceDoneResponse struct {

}



type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
