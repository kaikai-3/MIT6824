package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file "+filepath, err)
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
	}
	return &kva
}

func Taskcompeleted(task *Task) {
	reply := RpcReply{}
	call("Coordinator.Taskover", task, &reply)
}

func getTask() Task {
	args := RpcArgs{}
	reply := Task{}

	call("Coordinator.Assigntask", &args, &reply)
	return reply
}

func Domap(task *Task, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.Filename)
	if err != nil {
		log.Fatal("File reading error:"+task.Filename, err)
	}

	intermediates := mapf(task.Filename, string(content))

	buffer := make([][]KeyValue, task.Nreduce)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.Nreduce
		buffer[slot] = append(buffer[slot], intermediate)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < task.Nreduce; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.Taskid, i, &buffer[i]))
	}
	task.Intermediates = mapOutput
	Taskcompeleted(task)
}

func Doreduce(task *Task, reducef func(string, []string) string) {
	intermediates := *readFromLocalFile(task.Intermediates)

	sort.Sort(ByKey(intermediates))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediates[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.Taskid)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	Taskcompeleted(task)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		response := getTask()
		switch response.TaskType {
		case MAP:
			Domap(&response, mapf)
		case REDUCE:
			Doreduce(&response, reducef)
		case WAIT:
			time.Sleep(2 * time.Second)
		case OVER:
			return
		default:
			fmt.Printf("unknown worktype %v\n", response.TaskType)
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := RpcArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := RpcReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.RpcHandler", &args, &reply)
//
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %s\n", reply.Files)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

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
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	//fmt.Println("call Error", err)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
