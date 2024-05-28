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

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	RunFlag := true
	for RunFlag {
		TaskGet := CallForTask()
		switch TaskGet.TaskTyge {
		case "Map":
			{
				DoMapTask(mapf, TaskGet)
				CallForMissionDone(TaskGet)
				//time.Sleep(time.Second)
			}
		case "Reduce":
			{
				DoReduceTask(reducef, TaskGet)
				CallForMissionDone(TaskGet)
				//time.Sleep(time.Second)
			}
		case "Wait":
			{
				fmt.Println("Worker Waiting")
				time.Sleep(time.Second)
			}
		case "Kill":
			{
				fmt.Println("WorkerKilled")
				RunFlag = false
			}
		default:
			{
				fmt.Println("UnKnow Task!")
				fmt.Println(TaskGet)
				RunFlag = false
			}

		}

	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMapTask(mapf func(string, string) []KeyValue, TaskGet Task) {

	var intermediate []KeyValue
	filename := TaskGet.FileName[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	NReduce := TaskGet.NReduce
	HashedKV := make([][]KeyValue, NReduce)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%NReduce] = append(HashedKV[ihash(kv.Key)%NReduce], kv)
	}

	for i := 0; i < NReduce; i++ {
		filename := "mr-temp-" + strconv.Itoa(TaskGet.TaskId) + "-" + strconv.Itoa(i)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		file.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, TaskGet Task) {

	intermediate := getKeys(TaskGet.FileName)
	oname := "mr-out-" + strconv.Itoa(TaskGet.TaskId-TaskGet.NMaps)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func getKeys(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
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
	sort.Sort(ByKey(kva))
	return kva
}

func CallForTask() Task {

	// declare an argument structure.
	args := Args{}
	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.CallForTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply： %+v\n", reply)

	} else {
		fmt.Printf("call failed!\n")
	}

	return reply
}

func CallForMissionDone(TaskGet Task) {

	// declare an argument structure.
	args := Args{TaskGet.TaskId}
	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.CallForMissionDone", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply： %+v\n", reply)

	} else {
		fmt.Printf("call failed!\n")
	}
	return
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println("call error")
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
