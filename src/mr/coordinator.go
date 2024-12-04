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

type TASKSTATUS int
type TASKTYPE int
type STATE int

const (
	IDLE TASKSTATUS = iota
	INPROCESS
	COMPLETED
)

const (
	MAP TASKTYPE = iota
	REDUCE
	WAIT
	OVER
)

type Task struct {
	Taskid        int
	Filename      string
	Output        string
	TaskType      TASKTYPE
	Intermediates []string
	Nreduce       int
}

type CoordinatorTask struct {
	TaskStatus    TASKSTATUS //
	StartTime     time.Time  //To promise compeleted in reasional time
	TaskReference *Task
}

type Coordinator struct {
	// Your definitions here.
	TaskQueue        chan *Task //inprocessing task
	TaskList         map[int]*CoordinatorTask
	CoordinatorPhase TASKTYPE // map|reduce|over
	Nreduce          int
	InputFiles       []string   //files
	Intermediates    [][]string //intermediates result
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//

// start a thread that listens for RPCs from worker.go
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
	//fmt.Println("server listening at address:", l.Addr())
	go http.Serve(l, nil)
}

func (c *Coordinator) Assigntask(args *RpcArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskList[reply.Taskid].TaskStatus = INPROCESS
		c.TaskList[reply.Taskid].StartTime = time.Now()
	} else if c.CoordinatorPhase == OVER {
		*reply = Task{TaskType: OVER}
	} else {
		*reply = Task{TaskType: WAIT}
	}
	return nil
}

func (c *Coordinator) Taskover(task *Task, reply *RpcReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.TaskType != c.CoordinatorPhase || c.TaskList[task.Taskid].TaskStatus == COMPLETED {
		return nil
	}
	c.TaskList[task.Taskid].TaskStatus = COMPLETED
	go c.processTaskResult(task)
	return nil
}
func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskType {
	case MAP:
		for reduceTaskID, file := range task.Intermediates {
			c.Intermediates[reduceTaskID] = append(c.Intermediates[reduceTaskID], file)
		}
		if c.allTskDone() {
			c.createReduceTask()
			c.CoordinatorPhase = REDUCE
		}
	case REDUCE:
		if c.allTskDone() {
			c.CoordinatorPhase = OVER
		}
	}
}

func (c *Coordinator) allTskDone() bool {
	for _, task := range c.TaskList {
		if task.TaskStatus != COMPLETED {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.CoordinatorPhase == OVER
	return ret
}

func (c *Coordinator) createMapTask() {
	for index, filename := range c.InputFiles {
		taskList := Task{
			Filename: filename,
			TaskType: MAP,
			Nreduce:  c.Nreduce,
			Taskid:   index,
		}
		c.TaskQueue <- &taskList
		c.TaskList[index] = &CoordinatorTask{
			TaskStatus:    IDLE,
			TaskReference: &taskList,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	c.TaskList = make(map[int]*CoordinatorTask)
	for index, files := range c.Intermediates {
		TaskList := Task{
			TaskType:      REDUCE,
			Nreduce:       c.Nreduce,
			Taskid:        index,
			Intermediates: files,
		}
		c.TaskQueue <- &TaskList
		c.TaskList[index] = &CoordinatorTask{
			TaskStatus:    IDLE,
			TaskReference: &TaskList,
		}
	}
}

func (c *Coordinator) CheckTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.CoordinatorPhase == OVER {
			mu.Unlock()
			return
		}
		for _, ctask := range c.TaskList {
			if time.Now().Sub(ctask.StartTime) > 10*time.Second && ctask.TaskStatus == INPROCESS {
				c.TaskQueue <- ctask.TaskReference
				ctask.TaskStatus = IDLE
			}
		}
		mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		TaskList:         make(map[int]*CoordinatorTask),
		CoordinatorPhase: MAP,
		Nreduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}

	c.createMapTask()
	// Your code here
	go c.CheckTimeOut()
	c.server()

	return &c
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
