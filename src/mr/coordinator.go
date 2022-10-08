package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTaskQueue       map[int]Task
	reduceTaskQueue    map[int]Task
	nReduce            int
	nMap               int
	mux                sync.Mutex
	mapTaskProgress    map[int]Task
	reduceTaskProgress map[int]Task
	reduceReady        bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mux.Lock()
	defer c.mux.Unlock()
	// Your code here.
	if len(c.mapTaskQueue) == 0 && len(c.mapTaskProgress) == 0 && len(c.reduceTaskQueue) == 0 && len(c.reduceTaskProgress) == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTaskQueue:       make(map[int]Task),
		reduceTaskQueue:    make(map[int]Task),
		nReduce:            nReduce,
		nMap:               len(files),
		mapTaskProgress:    make(map[int]Task),
		reduceReady:        false,
		reduceTaskProgress: make(map[int]Task),
	}

	// Your code here.
	c.mux.Lock()
	defer c.mux.Unlock()
	fileNum := len(files)
	for i, file := range files {
		c.mapTaskQueue[i] = Task{
			MapFile:      file,
			TaskID:       i,
			TaskState:    Map,
			NMapTasks:    fileNum,
			NReduceTasks: nReduce,
			TimeStamp:    time.Now().Unix(),
		}
	}

	c.server()
	return &c
}

func (c *Coordinator) reassignStallTask() {
	curTime := time.Now().Unix()
	for k, v := range c.mapTaskProgress {
		if curTime-v.TimeStamp > 10 {
			c.mapTaskQueue[k] = v
			delete(c.mapTaskProgress, k)
			log.Println("reassign map task ", v.TaskID)
		}
	}
	for k, v := range c.reduceTaskProgress {
		if curTime-v.TimeStamp > 10 {
			c.reduceTaskQueue[k] = v
			delete(c.reduceTaskProgress, k)
			log.Println("reassign reduce task ", v.TaskID)
		}
	}
}

func (c *Coordinator) RequestTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.reassignStallTask()
	if len(c.mapTaskQueue) > 0 {
		for k, v := range c.mapTaskQueue {
			v.TimeStamp = time.Now().Unix()
			reply.Task = v
			// move to progress queue
			c.mapTaskProgress[k] = v
			delete(c.mapTaskQueue, k)
			log.Println("handle out map task ", v.TaskID)
			return nil
		}
	} else if len(c.mapTaskProgress) > 0 {
		reply.TaskState = Wait // tell worker to wait for map task complete
		return nil
	}

	//prepare reduce task
	if !c.reduceReady {
		for i := 0; i < c.nReduce; i++ {
			c.reduceTaskQueue[i] = Task{
				TaskState:    Reduce,
				TaskID:       i,
				TimeStamp:    time.Now().Unix(),
				NMapTasks:    c.nMap,
				NReduceTasks: c.nReduce,
			}
		}
		c.reduceReady = true
	}

	if len(c.reduceTaskQueue) > 0 {
		for k, v := range c.reduceTaskQueue {
			v.TimeStamp = time.Now().Unix()
			reply.Task = v
			c.reduceTaskProgress[k] = v
			delete(c.reduceTaskQueue, k)
			log.Println("handle out reduce task ", v.TaskID)
			return nil
		}
	} else if len(c.reduceTaskProgress) > 0 {
		reply.TaskState = Wait
	} else {
		reply.TaskState = Done
	}
	return nil
}

func (c *Coordinator) TaskDone(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	switch args.TaskState {
	case Map:
		log.Printf("map task: %d done", args.TaskID)
		delete(c.mapTaskProgress, args.TaskID)
	case Reduce:
		log.Printf("reduce task: %d done", args.TaskID)
		delete(c.reduceTaskProgress, args.TaskID)
	}
	return nil
}
