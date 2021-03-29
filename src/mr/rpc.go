package mr
import "os"
import "strconv"
//
// RPC definitions.
//
// remember to capitalize all names.
//


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

type TaskRequestArgs struct {
	Str string
}

type TaskRequestReply struct {
	// for both :)
	TaskTypo TaskType
	TaskId int
	// map task
	MapFile string
	NReduce int
	// reduce task
	NMap int
}

type TaskCompleteArgs struct {
	TaskTypo TaskType
	TaskId int
}

type TaskCompleteReply struct {
	Response bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

