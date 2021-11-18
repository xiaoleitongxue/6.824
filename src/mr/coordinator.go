package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mutex sync.Mutex
	files []string
	//the numbers of reduce Function
	nReduce   int
	taskPhase TaskPhase
	//等待执行的任务
	tasks            []Task
	taskExecuteInfos []TaskExecuteInfo
	done             bool
	lastWorkerId     int
	taskChannel      chan Task
}

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type TaskExecuteInfo struct {
	status    int
	workerId  int
	startTime time.Time
}

//RPC arguments, so variable start with capital
type Task struct {
	FileName string
	//The numbers of reduce function
	NReduce int
	//The numbers of Map function
	NMap int
	//TaskId has set when init
	TaskId int
	Phase  TaskPhase
	Alive  bool
}

const (
	TaskReady     = 0
	TaskQueue     = 1
	TaskRunning   = 2
	TaskCompleted = 3
	TaskError     = 4
)
const (
	//最大运行时间
	MaxTaskExecuteTime = time.Second * 5
	//轮询时间间隔
	ScheduleInterval = time.Millisecond * 500
)

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

	//Your code here.
	c.mutex.Lock()
	ret = c.done
	c.mutex.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.mutex = sync.Mutex{}
	c.nReduce = nReduce
	c.files = files
	c.lastWorkerId = 0
	c.done = false
	if nReduce > len(files) {
		c.taskChannel = make(chan Task, nReduce)
	} else {
		c.taskChannel = make(chan Task, len(c.files))
	}
	//init map task
	c.taskPhase = MapPhase
	c.taskExecuteInfos = make([]TaskExecuteInfo, len(c.files))
	//int map tasks
	//the task id correspond with taskExecuteInfo index
	c.tasks = make([]Task, len(c.files))
	for index, fileName := range files {
		c.tasks[index] = Task{
			FileName: fileName,
			NReduce:  c.nReduce,
			NMap:     len(files),
			TaskId:   index,
			Phase:    MapPhase,
			Alive:    true,
		}
	}
	go c.tickSchedule()
	c.server()
	return &c
}

func (c *Coordinator) tickSchedule() {
	for !c.Done() {
		c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (c *Coordinator) schedule() {
	c.mutex.Lock()
	if c.done {
		return
	}
	isAllTaskCompleted := true
	for index, taskExecuteInfo := range c.taskExecuteInfos {
		switch taskExecuteInfo.status {
		case TaskReady:
			isAllTaskCompleted = false
			c.taskChannel <- c.tasks[index]
			c.taskExecuteInfos[index].status = TaskQueue
		case TaskRunning:
			isAllTaskCompleted = false
			if time.Now().Sub(taskExecuteInfo.startTime) > MaxTaskExecuteTime {
				c.taskExecuteInfos[index].status = TaskQueue
			}
		case TaskCompleted:
		case TaskQueue:
			isAllTaskCompleted = false
		case TaskError:
			isAllTaskCompleted = false
			c.taskExecuteInfos[index].status = TaskQueue
		default:
			panic("task is not belong any status")
		}
	}

	if isAllTaskCompleted == true {
		if c.taskPhase == MapPhase {
			//init reduce task
			c.initReduceTask()
		} else {
			c.done = true
		}
	}
	c.mutex.Unlock()
}

func (c *Coordinator) initReduceTask() {
	c.taskPhase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	c.taskExecuteInfos = make([]TaskExecuteInfo,c.nReduce)
	for index := 0; index < c.nReduce; index++ {
		c.tasks[index] = Task{
			FileName: "",
			NReduce:  c.nReduce,
			NMap:     len(c.files),
			TaskId:   index,//which reduce
			Phase:    ReducePhase,
			Alive:    true,
		}
	}
}

func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := <-c.taskChannel
	reply.Task = &task
	if task.Alive == true {
		c.mutex.Lock()
		if task.Phase != c.taskPhase {
			panic("req task phase neq")
		}
		c.taskExecuteInfos[task.TaskId].status = TaskRunning
		c.taskExecuteInfos[task.TaskId].workerId = args.WorkerId
		c.taskExecuteInfos[task.TaskId].startTime = time.Now()
		c.mutex.Unlock()
	}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	if args.Done == true {
		c.taskExecuteInfos[args.TaskId].status = TaskCompleted
	} else {
		c.taskExecuteInfos[args.TaskId].status = TaskError
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.lastWorkerId += 1
	reply.WorkerId = c.lastWorkerId
	return nil
}
