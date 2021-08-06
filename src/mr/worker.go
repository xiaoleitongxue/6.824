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
	id int
	mapf func(string, string) []KeyValue
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := worker{}
	worker.mapf = mapf
	worker.reducef = reducef
	//向coordinator报告该worker想要注册
	// uncomment to send the Example RPC to the coordinator.
	worker.registerWorker()
	//启动worker
	worker.run()

}
func (w *worker) registerWorker() {
	args := &registerWorkerArgs{}
	reply := &registerWorkerReply{}
	if ok:=call("Coordinator.registerWorker",args,reply); !ok{
		log.Fatal("reg fail")
	}
	w.id = reply.workerId
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

func (w *worker) run(){
	for {
		task := w.requestATask()
		if !task.alive{
			return
		}
		w.doTask(task)
	}
}


func (w *worker) requestATask() Task{
	args := applyTaskArgs{}
	args.workerId = w.id
	reply := applyTaskReply{}
	if ok := call("Coordinator.assignATask",&args,&reply);!ok{
		os.Exit(1)
	}
	return *reply.task
}

func (w *worker) doTask(t Task) {
	DPrintf("in do Task")
	//通过task的类型确定执行那个阶段
	switch t.phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase err: %v", t.phase))
	}

}

func (w *worker) doMapTask(t Task) {
	//读文件
	contents, err := ioutil.ReadFile(t.fileName)
	//如果读取文件失败
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	kvs := w.mapf(t.fileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	//把具有相同key的键值对聚集到一起
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
    //写入文件
	for idx, l := range reduces {
		fileName := reduceName(t.taskId, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}

		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)

}
func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	//遍历map生成的所有文件
	for idx := 0; idx < t.NMaps; idx++ {
		fileName := reduceName(idx, t.taskId)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	//当所有Reduce都完成时，合并文件
	if err := ioutil.WriteFile(mergeName(t.taskId), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := reportTaskArgs{}
	args.done = done
	args.taskId = t.taskId
	args.phase = t.phase
	args.workerId = w.id
	reply := reportTaskReply{}
	if ok := call("Master.reportTask", &args, &reply); !ok {
		DPrintf("report task fail:%+v", args)
	}
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

