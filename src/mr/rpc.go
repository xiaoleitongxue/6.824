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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTaskArgs struct{
	WorkerId int
}

type RequestTaskReply struct{
	Task *Task
}

type ReportTaskArgs struct{
	Done bool
	TaskId int
	Phase TaskPhase
	WorkerId int
}

type ReportTaskReply struct{

}

type RegisterWorkerArgs struct{
	Message string
}

type RegisterWorkerReply struct{
	WorkerId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
