package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

const (
	//最大运行时间
	MaxTaskRunTime = time.Second * 5
	//轮询时间间隔
	ScheduleInterval = time.Millisecond * 500
)

type Coordinator struct {
	// Your definitions here.
	mutex          sync.Mutex //互斥访问信号量
	files          []string   //输入文件
	nReduce        int        //reduce worker的数量
	taskPhase      TaskPhase  //任务阶段
	taskStatistics []TaskStatistics
	done           bool      //整个过程是否完成标记位
	lastWorkerId   int       //上一个worker分配的id
	taskChannel    chan Task //任务管道
}

type TaskStatistics struct {
	//任务所处的状态
	status    int
	workerId  int
	startTime time.Time
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
	ret := true
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret = c.done
	return ret

	// c.mutex.Lock()
	// defer c.mutex.Unlock()
	// return c.done
}

//
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
	if nReduce > len(files) {
		c.taskChannel = make(chan Task, nReduce)
	} else {
		c.taskChannel = make(chan Task, len(c.files))
	}
	c.initMapTask()
	go c.tickSchedule()
	c.server()
	return &c
}

func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskStatistics = make([]TaskStatistics, len(c.files))
	for index, _ := range c.taskStatistics {
		//将所有task的状态初始化为ready
		c.taskStatistics[index].status = 0
	}
}
func (c *Coordinator) initReduceTask() {
	c.taskPhase = ReducePhase
	c.taskStatistics = make([]TaskStatistics, c.nReduce)
	// for index, _ := range c.taskStatistics {
	// 	//将所有task的状态初始化为ready
	// 	c.taskStatistics[index].status = 0
	// }
}

func (c *Coordinator) tickSchedule() {
	for !c.Done() {
		//任务调度
		c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//调度算法
func (c *Coordinator) schedule() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.done {
		return
	}
	//判断当前阶段的所有任务是否已完成
	isAllTaskFinishedInCurrentPhase := true

	for index, taskStat := range c.taskStatistics {
		switch taskStat.status {
		case TaskStatusReady:
			isAllTaskFinishedInCurrentPhase = false
			c.taskChannel <- c.generateATask(index)
			c.taskStatistics[index].status = TaskStatusQueue
		case TaskStatusQueue:
			isAllTaskFinishedInCurrentPhase = false
		case TaskStatusRunning:
			isAllTaskFinishedInCurrentPhase = false
			if time.Now().Sub(taskStat.startTime) > MaxTaskRunTime {
				c.taskStatistics[index].status = TaskStatusQueue
				c.taskChannel <- c.generateATask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			isAllTaskFinishedInCurrentPhase = false
			c.taskStatistics[index].status = TaskStatusQueue
			c.taskChannel <- c.generateATask(index)
		default:
			panic("task's not belong any status")
		}
	}

	if isAllTaskFinishedInCurrentPhase {
		if c.taskPhase == MapPhase {
			c.initReduceTask()
		} else {
			c.done = true
		}
	}
}

func (c *Coordinator) generateATask(taskId int) Task {
	task := Task{
		FileName: "",
		NReduce:  c.nReduce,
		NMaps:    len(c.files),
		TaskId:   taskId,
		Phase:    c.taskPhase,
		Alive:    true,
	}
	if task.Phase == MapPhase {

		task.FileName = c.files[taskId]

	}
	return task
}

//处理来自worker的rpc请求
func (c *Coordinator) AssignATask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	//从c.taskChannel中取出一个任务
	task := <-c.taskChannel
	reply.Task = &task

    if task.Alive{
		c.mutex.Lock()
	defer c.mutex.Unlock()
	if task.Phase != c.taskPhase {
		panic("req task phase neq")
	}
	c.taskStatistics[task.TaskId].status = TaskStatusRunning
	c.taskStatistics[task.TaskId].workerId = args.WorkerId
	c.taskStatistics[task.TaskId].startTime = time.Now()
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if (c.taskPhase != args.Phase) || (args.WorkerId != c.taskStatistics[args.TaskId].workerId) {
		return nil
	}
	if args.Done {
		c.taskStatistics[args.TaskId].status = TaskStatusFinish

	} else {
		c.taskStatistics[args.TaskId].status = TaskStatusErr
	}
	return nil
}

//管理worker
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.lastWorkerId += 1
	reply.WorkerId = c.lastWorkerId
	return nil
}
