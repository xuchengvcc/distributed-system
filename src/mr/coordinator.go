package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	// Reduces int

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MyHandler(args *ExampleArgs, reply *ExampleReply) error {
	return nil
}

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
	rpc.Register(c) // 注册c的方法 Example，server，Done
	rpc.HandleHTTP() // 注册HTTP路由
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() // 获取一个socket-name
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname) //创建了一个unix套接字，并监听，"unix"指定网络类型,l为监听对象
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)  // 启动一个HTTP服务器，第一个参数为实现了net.Listener接口的对象，第二个参数是http.Handler接口的实现，
	// 'nil'表示使用默认 HTTP 处理器
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// files: 文件名
	c := Coordinator{}

	// Your code here.
	// 1.创建出多个worker 
	// 2.将任务分配给不同的worker 
	// 3.worker内部执行Map 
	// 4.reducer执行Reduce并将结果写入文件


	c.server()
	return &c
}
