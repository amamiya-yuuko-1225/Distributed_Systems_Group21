package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"

	//"io"
	//"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := registerWorker()
	// send heartbeat to coordinator
	go sendHeartbeat(workerId)
	var args TaskRequest
	reply := TaskResponse{}
	reply.TaskType = "idle"
	for {
		// if all done, exit
		if reply.AllDone {
			log.Printf("All done, exiting...")
			return
		}
		if reply.TaskType == "idle" {
			time.Sleep(20 * time.Millisecond) // avoid flooding the coordinator
			args = TaskRequest{WorkerState: Idle, WorkerId: workerId}
		} else if reply.TaskType == "map" {
			execMap(reply.FileName, mapf, reply.MapId, reply.NReduce)
			args = TaskRequest{WorkerState: MapFinished, WorkerId: workerId, FileName: reply.FileName}
		} else {
			execReduce(reply.ReduceId, reducef, reply.MapCount)
			args = TaskRequest{WorkerState: ReduceFinished, WorkerId: workerId, ReduceId: reply.ReduceId}
		}
		ok := call("Coordinator.AllocateTasks", &args, &reply)
		// Coordinator failure deteced through the return value of RPC
		if !ok {
			log.Fatal("Coordinator failure detected, exiting...")
		}
	}
}

func execMap(filename string, mapf func(string, string) []KeyValue, mapId int, nReduce int) {
	// open file and read its content
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// call mapf() to handle content
	kvs := mapf(filename, string(content))

	// create and store intermediate file
	intermediateData := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		reduceId := ihash(kv.Key) % nReduce
		intermediateData[reduceId] = append(intermediateData[reduceId], kv)
	}

	// send intermediate file to corresponding reduce worker through RPC
	for i, data := range intermediateData {
		args := SendFileArgs{
			MapId:    mapId,
			ReduceId: i,
			Data:     serializeKeyValue(data), // serialize KeyValue
		}
		reply := SendFileReply{}
		ok := call("Coordinator.ReceiveMapOutput", &args, &reply)
		if !ok || !reply.Success {
			log.Fatalf("failed to send map output for reduce %d", i)
		}
	}
}

func execReduce(reduceId int, reducef func(string, []string) string, nMap int) {
	// fetch intermediate file from map workers through RPC
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		args := TaskRequest{
			WorkerState: Idle,
			ReduceId:    reduceId,
			MapId:       i,
		}
		reply := TaskResponse{}
		ok := call("Coordinator.FetchReduceInput", &args, &reply)
		if !ok {
			log.Fatalf("failed to fetch reduce input for reduce %d", reduceId)
		}
		data := deserializeKeyValue(reply.Data) // deserialize keyValue
		intermediate = append(intermediate, data...)
	}

	// sort by key
	sort.Sort(ByKey(intermediate))

	// open file
	oname := fmt.Sprintf("mr-out-%d.txt", reduceId)
	ofile, _ := os.Create(oname)

	// Group the data and call the reduce function to process it
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

// helper function
func serializeKeyValue(kvs []KeyValue) []byte {
	data, _ := json.Marshal(kvs)
	return data
}

// helper function
func deserializeKeyValue(data []byte) []KeyValue {
	var kvs []KeyValue
	json.Unmarshal(data, &kvs)
	return kvs
}

// send hearbeat
func sendHeartbeat(workerId int) {
	for {
		time.Sleep(10 * time.Millisecond)
		args := HeartRequest{WorkerId: workerId}
		reply := HeartReply{}
		call("Coordinator.ReceiveHeartbeat", &args, &reply)
	}
}

/**
 * @description: register this machine at coordinator
 * @return {int} workerID or -1
 */
func registerWorker() int {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		return reply.WorkerId
	}
	log.Fatal("Failed to register worker")
	return -1
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	c, err := rpc.DialHTTP("tcp", "amamiya-yuuko.ost.sgsnet.se:8888") // TCP socket instead of Unix socket
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
