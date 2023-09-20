package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	Lock      sync.Mutex
	FilesMap  map[string]int
	FileQueue []string
	TaskQueue map[string]Task
	Status    Schedule
	NReduce   int
	NMap      int
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) GetTask(args *ExampleArgs, reply *RpcTask) error {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	switch m.Status {
	// 结束任务，Worker退出。
	case FinishSchedule:
		reply.Status = FinishTask

	case MapSchedule:
		// 目前无任务，Worker静置等待。
		if len(m.FileQueue) == 0 {
			reply.Status = WaitingTask
			break
		}
		// 创建Map任务
		reply.Filename = m.FileQueue[0]
		reply.Status = MapTask
		reply.NReduce = m.NReduce
		reply.Idx = m.FilesMap[reply.Filename]

		task := Task{
			Filename:  reply.Filename,
			Status:    reply.Status,
			StartTime: time.Now(),
		}
		m.TaskQueue[reply.Filename] = task

		m.FileQueue = m.FileQueue[1:]

	case ReduceSchedule:
		// 目前无任务，Worker静置等待。
		if len(m.FileQueue) == 0 {
			reply.Status = WaitingTask
			break
		}
		// 创建Reduce任务
		task := Task{
			Filename:  reply.Filename,
			Status:    reply.Status,
			StartTime: time.Now(),
		}

		fileIdx := m.FileQueue[0]
		m.FileQueue = m.FileQueue[1:]

		m.TaskQueue[fileIdx] = task

		reply.Idx, _ = strconv.Atoi(fileIdx)
		reply.Status = ReduceTask
		reply.NReduce = m.NReduce
		reply.NMap = m.NMap
	}
	return nil
}

func (m *Master) WorkerDone(args *RpcTask, reply *ExampleReply) error {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	switch args.Status {

	case MapTask:
		delete(m.TaskQueue, args.Filename)
		//fmt.Printf("File %v MapTask %v Finish!\n", args.Filename, args.Idx)
		if m.Status == MapSchedule && len(m.TaskQueue) == 0 && len(m.FileQueue) == 0 {
			// 所有Map任务完成
			// 收集所有临时文件准备进行Reduce任务
			m.PrePareForReduce()
		}
	case ReduceTask:
		//fmt.Printf("ReduceTask %v Finish!\n", args.Idx)
		stringIdx := strconv.Itoa(args.Idx)
		delete(m.TaskQueue, stringIdx)
		if m.Status == ReduceSchedule && len(m.TaskQueue) == 0 && len(m.FileQueue) == 0 {
			m.PrePareForEnd()
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	go m.heartbeat()
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.Status == FinishSchedule {
		// 准备关闭rpc连接
		ret = true
		time.Sleep(10 * time.Second)
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	filesMap := make(map[string]int)
	for idx, filename := range files {
		filesMap[filename] = idx
	}

	ReduceIdxQueue := make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		ReduceIdxQueue[i] = i
	}

	m := Master{
		Lock:      sync.Mutex{},
		FilesMap:  filesMap,
		FileQueue: files,
		TaskQueue: make(map[string]Task),
		Status:    MapSchedule,
		NReduce:   nReduce,
		NMap:      len(files),
	}
	// Your code here.
	m.server()

	//fmt.Println("Master Start!")
	return &m
}

func (m *Master) heartbeat() {
	// 检测任务超时机制
	for {
		for fileName, task := range m.TaskQueue {
			if time.Since(task.StartTime) > 10*time.Second {
				//fmt.Printf("%v Task Timeout,Restart it!\n", fileName)
				m.Lock.Lock()
				delete(m.TaskQueue, fileName)
				m.FileQueue = append(m.FileQueue, fileName)
				m.Lock.Unlock()
			}
		}
		time.Sleep(3 * time.Second)
	}
}

func (m *Master) PrePareForReduce() {
	// 防止死锁 这里不能加锁！
	//fmt.Printf("MapTask Finish!\tMaster will go to Reduce!\n")
	m.Status = ReduceSchedule
	for i := 0; i < m.NReduce; i++ {
		m.FileQueue = append(m.FileQueue, strconv.Itoa(i))
	}
}

func (m *Master) PrePareForEnd() {
	//fmt.Printf("All Tasks Finish!\tMaster will stop work!\n")
	m.Status = FinishSchedule
}
