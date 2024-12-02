package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
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
			// log.Printf("All done, exiting...")
			return
		}
		if reply.TaskType == "idle" {
			time.Sleep(10 * time.Millisecond) // avoid flooding the coordinator
			args = TaskRequest{WorkerState: Idle, WorkerId: workerId}
		} else if reply.TaskType == "map" {
			execMap(reply.FileName, mapf, reply.MapId, reply.NReduce)
			args = TaskRequest{WorkerState: MapFinished, WorkerId: workerId, FileName: reply.FileName}
		} else if reply.TaskType == "reduce" {
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

func execMap(filename string, mapf func(string, string) []KeyValue, mapId int, nReduce int) bool {
	// open file.
	//TODO: convert to file passing using RPC or other method
	log.Printf("map %d starts", mapId)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	log.Printf("map %d a", mapId)
	// do map task
	kvs := mapf(filename, string(content))
	log.Printf("map %d b", mapId)
	//create intermediate JSON files
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// map output: mr-mapID(1to1 with file nameTemp)-reduceID
		nameTemp := fmt.Sprintf("mr-%d-%d_", mapId, i)
		intermediateFiles[i], err = os.Create(nameTemp)
		if err != nil {
			log.Fatalf("cannot create file %v", nameTemp)
		}
		encoders[i] = json.NewEncoder(intermediateFiles[i])
		defer intermediateFiles[i].Close()
	}
	log.Printf("map %d c", mapId)
	// iterate all KVs
	for _, kv := range kvs {
		// allocate reducer Hash(key)
		reduceId := ihash(kv.Key) % nReduce
		// store KV into JSON file with index reduceID
		err := encoders[reduceId].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv pair: %v", err)
		}

	}
	log.Printf("map %d d", mapId)
	for _, tempFile := range intermediateFiles {
		nameTemp := tempFile.Name()
		name := nameTemp[:len(nameTemp)-1]
		if _, err := os.Stat(name); err == nil {
			os.Remove(nameTemp)
		} else {
			os.Rename(nameTemp, name)
		}

	}
	log.Printf("map %d done", mapId)
	return true
}

func execReduce(reduceId int, reducef func(string, []string) string, nMap int) bool {
	log.Printf("reduce %d starts", reduceId)
	// read all JSON files with specific reduceID
	// and convert them into KV[]
	// TODO: passing files on Internet
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		name := fmt.Sprintf("mr-%d-%d", i, reduceId)
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open file %v", name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatalf("Decode error: %v", err)
				}
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Shuffling/Grouping stage
	// First, sort all KVs by keys
	sort.Sort(ByKey(intermediate))
	//reduce output: mr-out-reduceID
	oname := fmt.Sprintf("mr-out-%d.txt_", reduceId)
	ofile, _ := os.Create(oname)

	//Group all KVs with a same key, and pass to reduce function
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// all KVs with a same key are grouped into "values"
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// do reduce task
		output := reducef(intermediate[i].Key, values)
		// write to output file
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		nameTemp := oname
		name := nameTemp[:len(nameTemp)-1]
		if _, err := os.Stat(name); err == nil {
			os.Remove(nameTemp)
		} else {
			os.Rename(nameTemp, name)
		}
		i = j
	}
	log.Printf("reduce %d done", reduceId)
	return true
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
