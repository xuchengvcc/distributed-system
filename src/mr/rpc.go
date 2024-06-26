package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

<<<<<<< HEAD
import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
=======
import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
// var uid *int
>>>>>>> xucheng-240506-lab1

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct {
	Task Task
}

type Reply struct {
	Task Task
}

// Add your RPC definitions here.
// func myCoordinatorSock() string {
// 	s := "/var/tmp/5840-mr-"
// 	s += strconv.Itoa(*uid)
// 	*uid++
// 	return s
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
