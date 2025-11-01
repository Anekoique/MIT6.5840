package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgess
	Completed
)

type JobPhase int

const (
	MapPhase JobPhase = iota
	ReducePhase
	Done
)

type Task struct {
	fileName string
	state    TaskState
}

type Coordinator struct {
	nMap    int
	nReduce int
	tasks   []*Task
	phase   JobPhase
	mutex   sync.Mutex
	cond    *sync.Cond
}

func (c *Coordinator) nextPhase() {
	c.phase += 1
	if c.phase == ReducePhase {
		c.tasks = make([]*Task, c.nReduce)
		for i := 0; i < c.nReduce; i++ {
			c.tasks[i] = &Task{
				state: Idle,
			}
		}
	}
}

func (c *Coordinator) updatePhase() {
	if c.phase == Done {
		return
	}
	for _, task := range c.tasks {
		if task.state != Completed {
			return
		}
	}
	c.nextPhase()
	c.cond.Broadcast()
}

// Dispatch RPC handlers for the worker to call.
func (c *Coordinator) Dispatch(args *WorkerArgs, reply *WorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for {
		switch c.phase {
		case MapPhase:
			if !c.assignMap(reply) {
				c.cond.Wait()
				continue
			}
		case ReducePhase:
			if !c.assignReduce(reply) {
				c.cond.Wait()
				continue
			}
		case Done:
		default:
			panic(fmt.Sprintf("Unexpected JobPhase %v", c.phase))
		}

		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Phase = c.phase
		return nil
	}
}

func (c *Coordinator) HandleDone(args *DoneArgs, reply *WorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.MapTaskID != -1 {
		c.tasks[args.MapTaskID].state = Completed
	}
	if args.ReduceTaskID != -1 {
		c.tasks[args.ReduceTaskID].state = Completed
	}
	c.updatePhase()
	return nil
}

func (c *Coordinator) assignMap(reply *WorkerReply) bool {
	for i, task := range c.tasks {
		if task.state == Idle {
			reply.ID = i
			reply.FileName = task.fileName
			c.assignTask(task)
			return true
		}
	}
	return false
}

func (c *Coordinator) assignReduce(reply *WorkerReply) bool {
	for i, task := range c.tasks {
		if task.state == Idle {
			reply.ID = i
			c.assignTask(task)
			return true
		}
	}
	return false
}

func (c *Coordinator) assignTask(task *Task) {
	task.state = InProgess
	go func(task *Task) {
		timedue := time.After(10 * time.Second)
		<-timedue
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if task.state != Completed {
			task.state = Idle
			c.cond.Broadcast()
		}
	}(task)
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.phase == Done
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = MapPhase
	c.tasks = make([]*Task, c.nMap)
	c.cond = sync.NewCond(&c.mutex)
	for i, file := range files {
		c.tasks[i] = &Task{
			fileName: file,
			state:    Idle,
		}
	}

	c.server()
	return &c
}
