package mr

<<<<<<< HEAD
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
=======
import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int
type Phase int

const OutTime = 10 * time.Second

const (
	Free TaskState = iota
	Processing
	Completed
)

const (
	Map Phase = iota
	Reduce
	Wait
	Done
)

type MasterTask struct {
	TaskState TaskState
	Reference *Task
}

type Task struct {
	Id         int
	FileName   string
	OFileNames []string
	TaskPhase  Phase
	NReducer   int
	NMapper    int
	StartTime  time.Time
	EndTime    time.Time
	next       *Task
	pre        *Task
}

type DubTaskList struct {
	size uint32
	head *Task
	tail *Task
}

func (dtl *DubTaskList) addToTail(task *Task) {
	if task == nil {
		log.Fatalf("任务为空，添加任务失败")
	}
	dtl.size++
	if dtl.size == 1 {
		dtl.head = task
		dtl.tail = task
		task.next = task
		task.pre = task
		return
	}
	dtl.tail.next = task
	task.next = dtl.tail.next
	task.pre = dtl.tail
	dtl.tail = task
}

func (dtl *DubTaskList) pickHead() (task *Task) {
	task = dtl.head
	if dtl.size == 0 {
		return task
	} else if dtl.size == 1 {
		dtl.head = nil
		dtl.tail = nil
	} else {
		dtl.head = task.next
		dtl.tail.next = task.next
		task.next.pre = dtl.tail
	}
	task.next = nil
	task.pre = nil
	dtl.size--
	return task
}

type Coordinator struct {
	// Your definitions here.
	NMapper  int
	NReducer int
	Phase    Phase //当前所处阶段
	// TaskQueue chan *Task
	TaskQueue      DubTaskList
	TaskMeta       map[int]*MasterTask
	Files          []string
	InterFileNames [][]string
	Mutex          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	// 处理 worker call的逻辑，
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.TaskQueue.size > 0 {
		task := c.TaskQueue.pickHead()
		c.TaskMeta[task.Id].TaskState = Processing
		task.StartTime = time.Now()
		reply.Task = *task
	} else if c.Phase == Done {
		reply.Task = Task{
			TaskPhase: Done,
		}
	} else {
		reply.Task = Task{
			TaskPhase: Wait,
		}
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *Args, reply *Reply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.Task.TaskPhase != c.Phase || c.TaskMeta[args.Task.Id].TaskState == Completed {
		// 收到不同阶段的结果或是重复结果，丢弃
		return nil
	}
	c.TaskMeta[args.Task.Id].TaskState = Completed
	c.ProcessTaskComplete(&args.Task)
	return nil
}

func (c *Coordinator) ProcessTaskComplete(task *Task) {
	// c.Mutex.Lock()
	// defer c.Mutex.Unlock()
	// fmt.Println("commit task completed")
	switch task.TaskPhase {
	case Map:
		c.InterFileNames[task.Id] = task.OFileNames
		if c.checkCompleted() {
			// 所有map任务执行完毕
			c.makeReduceTask()
			c.Phase = Reduce
		}
	case Reduce:
		if c.checkCompleted() {
			c.Phase = Done
		}
	}
}

func (c *Coordinator) checkCompleted() (completed bool) {
	for _, masterTask := range c.TaskMeta {
		if masterTask.TaskState != Completed {
			return false
		}
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
>>>>>>> xucheng-240506-lab1
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

<<<<<<< HEAD

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

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
=======
// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)  // 注册c的方法 Example，server，Done
	rpc.HandleHTTP() // 注册HTTP路由
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() // 获取一个socket-name
	// registerSockname := sockname + "_register"
	os.Remove(sockname)
	// os.Remove(registerSockname)          //用于监听注册 worker
	l, e := net.Listen("unix", sockname) //创建了一个unix套接字，并监听，"unix"指定网络类型,l为监听对象
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// lReg, e := net.Listen("unix", registerSockname)
	// if e != nil {
	// 	log.Fatal("listen error:", e)
	// }
	// go http.Serve(lReg, nil)
	go http.Serve(l, nil) // 启动一个HTTP服务器，第一个参数为实现了net.Listener接口的对象，第二个参数是http.Handler接口的实现，
	// 'nil'表示使用默认 HTTP 处理器
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// Your code here.
	ret := c.Phase == Done
	return ret
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(4 * time.Second)
		c.Mutex.Lock()
		if c.Phase == Done {
			return
		}
		for _, masterTask := range c.TaskMeta {
			if masterTask.TaskState == Processing && time.Since(masterTask.Reference.StartTime) > OutTime {
				c.TaskQueue.addToTail(masterTask.Reference)
				masterTask.TaskState = Free
			}
		}
		c.Mutex.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// files: 文件名
	// dir, err := os.Getwd()
	// if err != nil {
	// 	log.Fatal("获取当前目录失败:", err)
	// 	return nil
	// }

	// pattern := "mr-*"
	// deletFiles, err := filepath.Glob(filepath.Join(dir, pattern))
	// if err != nil {
	// 	log.Fatal("Error finding files:", err)
	// 	return nil
	// }

	// for _, deletFile := range deletFiles {
	// 	err := os.Remove(deletFile)
	// 	if err != nil {
	// 		log.Fatal("Error deleting file:", deletFile, err)
	// 	}
	// }

	// matches, err := filepath.Glob(filepath.Join(dir, files[0]))
	// if err != nil {
	// 	log.Fatal("匹配文件失败:", err)
	// 	return nil
	// }
	// c := Coordinator{
	// 	NReducer:       nReduce,
	// 	NMapper:        len(matches),
	// 	Files:          matches,
	// 	Phase:          Map,
	// 	TaskQueue:      DubTaskList{size: 0},
	// 	TaskMeta:       make(map[int]*MasterTask),
	// 	InterFileNames: make([][]string, len(matches)),
	// }
	c := Coordinator{
		NReducer:       nReduce,
		NMapper:        len(files),
		Files:          files,
		Phase:          Map,
		TaskQueue:      DubTaskList{size: 0},
		TaskMeta:       make(map[int]*MasterTask),
		InterFileNames: make([][]string, len(files)),
	}

	// Your code here.
	// 1.创建出多个worker
	c.makeMapTask()
	c.server()
	// 这里用一个goroutine检查超时任务
	go c.catchTimeOut()
	return &c
}

func (c *Coordinator) makeMapTask() {
	// 创建Map任务列表，每个文件构建一个任务
	for idx, filename := range c.Files {
		taskMap := Task{
			FileName:  filename,
			TaskPhase: Phase(Map),
			NReducer:  c.NReducer,
			Id:        idx,
		}
		c.TaskQueue.addToTail(&taskMap)
		c.TaskMeta[idx] = &MasterTask{
			TaskState: TaskState(Free),
			Reference: &taskMap,
		}
	}
}

func (c *Coordinator) getInterFileName(idx int) []string {
	onames := make([]string, c.NMapper)
	for i, InterFileName := range c.InterFileNames {
		onames[i] = InterFileName[idx]
	}
	return onames
}

func (c *Coordinator) makeReduceTask() {
	// 创建Reduce任务列表，文件名传给reducetask
	for idx := range c.NReducer {
		taskMap := Task{
			OFileNames: c.getInterFileName(idx),
			TaskPhase:  Phase(Reduce),
			// NReducer:   c.NReducer,
			NMapper: c.NMapper,
			Id:      idx + c.NMapper,
		}
		c.TaskQueue.addToTail(&taskMap)
		c.TaskMeta[idx+c.NMapper] = &MasterTask{
			TaskState: TaskState(Free),
			Reference: &taskMap,
		}
	}
}
>>>>>>> xucheng-240506-lab1
