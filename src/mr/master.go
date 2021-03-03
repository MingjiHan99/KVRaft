package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskType int32

const (

	TaskType_Map TaskType = 0
	TaskType_Reduce TaskType = 1
	TaskType_Empty TaskType = 2
	TaskType_Exit TaskType = 3
)

type Task struct {
	// both 
	taskType TaskType 
	taskId int
	// Map Task
	mapFile string
	
	// Status	
	isDistributed bool
	isCompleted bool
}

type Master struct {
	// Your definitions here.
	mutex sync.Mutex
	files []string
	nMap int
    nReduce int
	mapCompletedNum int
	reduceCompletedNum int	
	mapTask []Task
	reduceTask []Task
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) MontiorTask(t TaskType, taskId int) {
	time.Sleep(10 * time.Second)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch t {
	case TaskType_Map:
		if !m.mapTask[taskId].isCompleted {
			log.Printf("Map task %v takes too long time. Recycle!",taskId)
			m.mapTask[taskId].isDistributed = false
		}
		
	case TaskType_Reduce:
		if !m.reduceTask[taskId].isCompleted {
			log.Printf("Reduce task %v takes too long time. Recycle!",taskId)
			m.reduceTask[taskId].isDistributed = false
		}
		
	}



}
func (m *Master) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	log.Printf("Received taskRequest rpc\n")
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// assign map tasks first
	for i := 0; i < m.nMap; i++ {
		if m.mapTask[i].isDistributed == false {
			log.Printf("Send Map task %v  path: %v\n", m.mapTask[i].taskId, m.mapTask[i].mapFile)
			reply.TaskTypo = m.mapTask[i].taskType
			reply.TaskId = m.mapTask[i].taskId
			reply.MapFile = m.mapTask[i].mapFile
			reply.NReduce = m.nReduce
			m.mapTask[i].isDistributed = true
			go m.MontiorTask(m.mapTask[i].taskType, m.mapTask[i].taskId)
			return nil
		}
	}
	// the reduce tasks can be assigned only when all map tasks are done 
	if m.mapCompletedNum == m.nMap {
		for i := 0; i < m.nReduce; i++ {
			if m.reduceTask[i].isDistributed == false {
				log.Printf("Send Reduce task %v \n", m.reduceTask[i].taskId)
				reply.TaskTypo = m.reduceTask[i].taskType
				reply.TaskId = m.reduceTask[i].taskId
				reply.NMap = m.nMap
				m.reduceTask[i].isDistributed = true
				go m.MontiorTask(m.reduceTask[i].taskType, m.reduceTask[i].taskId)
			
				return nil
			}
		}
	} 
	// if the reduce phrase is done , then we can let worker exit
	if m.reduceCompletedNum == m.nReduce {
		reply.TaskTypo = TaskType_Exit
	} else { 
		// if the map phrase is not done, we can only assign an empty task
		reply.TaskTypo = TaskType_Empty
	}
	
	return nil
}

func (m *Master) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	log.Printf("Received taskComplete rpc\n")
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch args.TaskTypo {
		case TaskType_Map:
			m.mapTask[args.TaskId].isCompleted = true
			m.mapCompletedNum += 1
		case TaskType_Reduce:
			m.reduceTask[args.TaskId].isCompleted = true
			m.reduceCompletedNum += 1
	}
	reply.Response = true
	
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	log.Printf("Received rpc\n")
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// when reduce phrase is done, the master can return
	if m.reduceCompletedNum == m.nReduce {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// init
	m.files = files
	m.nReduce = nReduce
	m.mapCompletedNum = 0
	m.reduceCompletedNum = 0
	m.nMap = len(files)
	m.mapTask = []Task{}
	m.reduceTask = []Task{}
	// Your code here.
	// put map tasks
	for i := 0; i < m.nMap; i++ {
		log.Printf("Map file: %v", m.files[i])
		m.mapTask = append(m.mapTask, 
		Task {
			taskType: TaskType_Map,
			mapFile: m.files[i],
			taskId: i,
			isDistributed: false,
			isCompleted: false })
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTask = append(m.reduceTask, 
		Task {
			taskType: TaskType_Reduce,
			mapFile: "",
			taskId: i,
			isDistributed: false,
			isCompleted: false })
	}
	log.Printf("Master started now: %v Map tasks %v Reduce tasks\b", 
				m.nMap, len(m.reduceTask))
	m.server()
	return &m
}
