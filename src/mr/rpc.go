package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type State int

const (
	Map    State = 1
	Reduce State = 2
	Done   State = 3
	Wait   State = 4
)

type GetTaskArgs struct {
	Task
}

type GetTaskReply struct {
	Task
}

type Task struct {
	TaskState    State //map or reduce or done
	TaskID       int
	NReduceTasks int    // map write file reduce buckets
	MapFile      string // map file location
	NMapTasks    int    // read map files
	TimeStamp    int64
}

type FinishedTaskArgs struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
