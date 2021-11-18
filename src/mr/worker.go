package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := worker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.registerWorker()
	worker.start()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
func (w *worker) registerWorker() {
	args := &RegisterWorkerArgs{}
	args.Message = "register worker"
	reply := &RegisterWorkerReply{}
	if ok := call("Coordinator.RegisterWorker", args, reply); !ok {
		log.Fatal("register worker fail")
	}
	w.id = reply.WorkerId

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (w *worker) start() {
	for {
		task := w.requestTask()
		if task.Alive == false {
			return
		}
		w.executeTask(task)
	}
}

func (w *worker) requestTask() Task {
	args := &RequestTaskArgs{}
	args.WorkerId = w.id
	reply := &RequestTaskReply{}
	if ok := call("Coordinator.AssignTask", args, reply); !ok {
		os.Exit(1)
	}
	return *reply.Task
}

func (w *worker) executeTask(task Task) {
	switch task.Phase {
	case MapPhase:
		w.executeMap(task)
	case ReducePhase:
		w.executeReduce(task)
	default:
		panic(fmt.Sprintf("task phase error:%v", task.Phase))
	}
}

func (w *worker) executeMap(task Task) {

	contents, err := ioutil.ReadFile(task.FileName)

	if err != nil {
		w.reportTask(task, false, err)
	}
	kvs := w.mapf(task.FileName,string(contents))
	reduces := make([][]KeyValue, task.NReduce)

	for _, kv := range kvs{
		columnIndex := ihash(kv.Key) % task.NReduce
		reduces[columnIndex] = append(reduces[columnIndex],kv)
	}
	//write each column to file
	for columnIndex, content := range reduces{
		fileName := reduceName(task.TaskId,columnIndex)
		file,err := os.Create(fileName)
		if err != nil{
			w.reportTask(task,false,err)
			return
		}
		encoder := json.NewEncoder(file)
		for _, kv :=range content{
			if err := encoder.Encode(&kv);err != nil{
				w.reportTask(task, false,err)
			}
		}
	}
	w.reportTask(task, true, err)
}



func (w *worker) executeReduce(task Task) {
	maps := make(map[string][]string)

	for mapIndex:=0; mapIndex<task.NMap;mapIndex++{
		fileName := reduceName(mapIndex,task.TaskId)
		file,err := os.Open(fileName)
		if err != nil {
			w.reportTask(task, false,err)
		}
		decoder := json.NewDecoder(file)
		for{
			var kv KeyValue
			if err := decoder.Decode(&kv);err !=nil{
				break
			}
			if _,ok:= maps[kv.Key];!ok{
				maps[kv.Key] = make([]string,0,100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string,0,100)
	for k,v := range maps{
		res = append(res, fmt.Sprintf("%v %v\n",k,w.reducef(k,v)))
	}

	if err := ioutil.WriteFile(mergeName(task.TaskId),[]byte(strings.Join(res, "")), 0600);err!=nil{
		w.reportTask(task,false,err)
	}
	w.reportTask(task,true,nil)
}

func (w *worker) reportTask(task Task, done bool, err error) {
	if err != nil{
		log.Printf("%v",err)
	}
	args := &ReportTaskArgs{
		Done : done,
		TaskId : task.TaskId,
		Phase : task.Phase,
		WorkerId : w.id,
	}
	reply := &ReportTaskReply{}
	if ok:= call("Coordinator.ReportTask",args,reply);!ok{
		log.Fatalf("worker do task fail")
	}


}

func reduceName(mapIndex int, reduceIndex int) string{
	return fmt.Sprintf("mr-%d-%d",mapIndex,reduceIndex)
}

func mergeName(reduceIndex int) string{
	return fmt.Sprintf("mr-out-%d",reduceIndex)
}

