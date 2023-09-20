package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(doMapTask func(string, string) []KeyValue,
	doReduceTask func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	// 轮询从Master处获取工作。
	for {
		task := GetMasterTask()
		switch task.Status {
		case MapTask:
			WorkerMap(doMapTask, task)
		case ReduceTask:
			WorkerReduce(doReduceTask, task)
		case WaitingTask:
			time.Sleep(3 * time.Second)
		case FinishTask:
			return
		default:
			fmt.Printf("got Unexcepted Task Status %v", task.Status)
		}
	}
}

func GetMasterTask() RpcTask {
	// send the RPC request, wait for the reply.
	args := ExampleArgs{}
	reply := RpcTask{}
	call("Master.GetTask", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func WorkerMap(mapf func(string, string) []KeyValue, task RpcTask) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()

	intermediate := mapf(task.Filename, string(content))
	TempFiles := make([]*os.File, task.NReduce)
	TempEncoders := make([]*json.Encoder, task.NReduce)

	for i := range TempFiles {
		TempFiles[i], err = os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("Create TempFiles Error %v", err.Error())
		}
		TempEncoders[i] = json.NewEncoder(TempFiles[i])
	}

	for _, kv := range intermediate {
		idx := ihash(kv.Key) % task.NReduce
		err := TempEncoders[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("Encoder Error Occur %v", err.Error())
		}
	}

	for idx, file := range TempFiles {
		NewFileName := "mr-" + strconv.Itoa(task.Idx) + "-" + strconv.Itoa(idx)

		err := os.Rename(file.Name(), NewFileName)
		if err != nil {
			log.Fatalf("OS Rename Error Occur %v", err.Error())
		}
	}
	WorkerDoneCall(task)
}

func WorkerDoneCall(task RpcTask) {
	call("Master.WorkerDone", &task, &ExampleReply{})
}

func WorkerReduce(reducef func(string, []string) string, task RpcTask) {
	intermediate := []KeyValue{}

	for MapIdx := 0; MapIdx < task.NMap; MapIdx++ {
		filename := "mr-" + strconv.Itoa(MapIdx) + "-" + strconv.Itoa(task.Idx)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v\nError:%v", filename, err.Error())
		}
		// 解码json
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	// 合并相邻相同键值对，并存入文件，命名为mr-out-idx
	SaveFileName := "mr-out-" + strconv.Itoa(task.Idx)
	SaveFile, _ := os.Create(SaveFileName)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(SaveFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	SaveFile.Close()
	WorkerDoneCall(task)
}
