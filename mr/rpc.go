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

type WorkerArgs struct{}

type WorkerReply struct {
	ID       int
	NMap     int
	NReduce  int
	FileName string
	Phase    JobPhase
}

type DoneArgs struct {
	MapTaskID    int
	ReduceTaskID int
}

type DoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
