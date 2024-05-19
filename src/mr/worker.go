package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerState struct {
	State bool
}

type MapWorker struct {
	FileName      string
	InterFileName []string //中间结果的文件名
	ID            int
}

func queryTask() (task *Task) {
	// fmt.Println("Start to query task")
	args := Args{}
	reply := Reply{}
	call("Coordinator.AssignTask", args, &reply)
	// fmt.Printf("queried task filename %v \n", reply.Task.FileName)
	return &reply.Task
}

func readFile(filename string) []uint8 {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// fmt.Println("worker starts working")
	for {
		task := queryTask()
		// fmt.Printf("task.TaskPhase:%v", task.TaskPhase)
		// fmt.Printf("task.TaskPhase:%v Map flag:%v\n Reduce flag:%v \n Wait flag:%v \n Done flag:%v \n ", task.TaskPhase, Map, Reduce, Wait, Done)
		switch task.TaskPhase {
		case Map:
			mapper(task, mapf)
		case Reduce:
			reducer(task, reducef)
		case Wait:
			time.Sleep(3 * time.Second)
		case Done:
			return
		}
		// 定时轮询任务
		// time.Sleep(2 * time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func writeToFile(taskId int, splitId int, keyValues []KeyValue) (oname string) {
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-temp-*")
	// fmt.Printf("创建临时文件：%v \n", tempFile)
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range keyValues {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	oname = fmt.Sprintf("mr-%v-%v", taskId, splitId)
	os.Rename(tempFile.Name(), oname)
	oname = filepath.Join(dir, oname)
	return oname
}

func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		// fmt.Printf("打开文件：%v \n", filename)
		if err != nil {
			log.Fatal("Failed to open file:", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
		// os.Remove(filename)
	}
	return &kva
}

func taskComplete(task *Task) {
	args := Args{Task: *task}
	reply := Reply{}
	// fmt.Printf("task %v completed\n", task.Id)
	call("Coordinator.CompleteTask", args, &reply)
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// fmt.Printf("执行mapper任务,准备打开文件:%v \n", task.FileName)
	filename := task.FileName
	content := readFile(filename)
	intermediates := []KeyValue{}
	// fmt.Printf("phase: %v, task id = %v starts working\n", task.TaskPhase, task.Id)
	kva := mapf(filename, string(content))
	// fmt.Printf("phase: %v, task id = %v still working\n", task.TaskPhase, task.Id)
	intermediates = append(intermediates, kva...)
	// 用环形缓冲区先缓存数据
	buffer := make([][]KeyValue, task.NReducer)
	for _, inteintermediate := range intermediates {
		split := ihash(inteintermediate.Key) % task.NReducer
		buffer[split] = append(buffer[split], inteintermediate)
	}
	outFileName := make([]string, 0)
	for i := range task.NReducer {
		outFileName = append(outFileName, writeToFile(task.Id, i, buffer[i]))
	}
	// 把outFileName告诉master
	task.OFileNames = outFileName
	taskComplete(task)
}

func reducer(task *Task, reducef func(string, []string) string) {
	// intermediates := []KeyValue{}
	kva := readFromLocalFile(task.OFileNames)
	sort.Sort(ByKey(*kva))

	dir, _ := os.Getwd()
	oname := filepath.Join(dir, fmt.Sprintf("mr-out-%v", task.Id-task.NMapper))
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	i := 0
	// fmt.Printf("phase: %v, task id = %v starts working\n", task.TaskPhase, task.Id)
	for i < len(*kva) {
		j := i + 1
		for j < len(*kva) && (*kva)[j].Key == (*kva)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (*kva)[k].Value)
		}
		output := reducef((*kva)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", (*kva)[i].Key, output)

		i = j
	}
	// fmt.Printf("phase: %v, task id = %v still working\n", task.TaskPhase, task.Id)
	taskComplete(task)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply) //改成MyHandler处理函数
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close() //在执行完call后关闭rpc连接
	// fmt.Printf("connect to %v : %v \n", sockname, rpcname)
	err = c.Call(rpcname, args, reply) // 这里执行了 Coordinator.Example
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
