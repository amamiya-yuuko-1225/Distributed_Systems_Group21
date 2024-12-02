package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net"

	//"io"
	//"io/ioutil"
	"errors"
	"log"
	"net/http"
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

const (
	coordinatorDest = "amamiya-yuuko.ost.sgsnet.se:8888"
	myDest          = "yichen.hogs.sgsnet.se:8888"
)

type Files struct {
	myDest string // ip:port of this worker
}

// Other workers call this by RPC to get file on this machine
func (f *Files) GetIntermidiateFile(arg *FetchReduceInputArgs, reply *FetchReduceInputReply) error {
	name := fmt.Sprintf("mr-%d-%d", arg.MapId, arg.ReduceId)
	if !fileExists(name) {
		return errors.New("file missing")
	}
	file, err := os.Open(name)
	if err != nil {
		log.Fatalf("cannot open file %v", name)
	}
	intermediate := []KeyValue{}
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
	reply.Data = intermediate
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	f := Files{
		myDest: myDest,
	}
	f.server()

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
			time.Sleep(1000 * time.Millisecond) // avoid flooding the coordinator
			args = TaskRequest{WorkerState: Idle, WorkerId: workerId}
		} else if reply.TaskType == "map" {
			execMap(reply.FileName, mapf, reply.MapId, reply.NReduce)
			args = TaskRequest{WorkerState: MapFinished, WorkerId: workerId, FileName: reply.FileName}
		} else {
			execReduce(reply.ReduceId, reducef, reply.MapCount, reply.MapOutputDest)
			args = TaskRequest{WorkerState: ReduceFinished, WorkerId: workerId, ReduceId: reply.ReduceId}
		}
		ok := call("Coordinator.AllocateTasks", coordinatorDest, &args, &reply)
		// Coordinator failure deteced through the return value of RPC
		if !ok {
			log.Fatal("Coordinator failure: allocate task failed, exiting...")
		}
	}
}

func execMap(filename string, mapf func(string, string) []KeyValue, mapId int, nReduce int) {
	log.Printf("map %d starts", mapId)
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
	// do map task
	kvs := mapf(filename, string(content))

	//create intermediate JSON files
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// create temp files, in order to prevent corrupt data being fetched
		nameTemp := fmt.Sprintf("mr-%d-%d_", mapId, i)
		intermediateFiles[i], err = os.Create(nameTemp)
		if err != nil {
			log.Fatalf("cannot create file %v", nameTemp)
		}
		encoders[i] = json.NewEncoder(intermediateFiles[i])
	}
	// iterate all KVs, write to temp files
	for _, kv := range kvs {
		// allocate reducer Hash(key)
		reduceId := ihash(kv.Key) % nReduce
		// store KV into JSON file with index reduceID
		err := encoders[reduceId].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv pair: %v", err)
		}

	}
	// move temp files to final map output files
	for _, tempFile := range intermediateFiles {
		nameTemp := tempFile.Name()
		name := nameTemp[:len(nameTemp)-1]
		if fileExists(name) {
			os.Remove(nameTemp)
		} else {
			os.Rename(nameTemp, name)
		}
	}
	log.Printf("map %d done", mapId)
}

func execReduce(reduceId int, reducef func(string, []string) string, nMap int, dests map[int]string) {
	// fetch intermediate file from map workers through RPC
	// iterate ALL workers who have done "map" to fetch the needed file
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		args := FetchReduceInputArgs{
			ReduceId: reduceId,
			MapId:    i,
		}
		reply := FetchReduceInputReply{}
		ok := call("Files.GetIntermidiateFile", dests[i], &args, &reply)
		if !ok {
			log.Fatalf("Fetch intermidiate files failure: mr-%d-%d", i, reduceId)
		}
		intermediate = append(intermediate, reply.Data...)
	}

	// sort by key
	sort.Sort(ByKey(intermediate))

	// open file
	oname := fmt.Sprintf("mr-out-%d.txt", reduceId)
	ofile, _ := os.Create(oname)
	log.Printf("output %s", oname)
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

// send hearbeat
func sendHeartbeat(workerId int) {
	for {
		time.Sleep(200 * time.Millisecond)
		args := HeartRequest{WorkerId: workerId}
		reply := HeartReply{}
		call("Coordinator.ReceiveHeartbeat", coordinatorDest, &args, &reply)
	}
}

/**
 * @description: register this machine at coordinator
 * @return {int} workerID or -1
 */
func registerWorker() int {
	args := RegisterArgs{}
	args.WorkerDest = myDest
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", coordinatorDest, &args, &reply)
	if ok {
		return reply.WorkerId
	}
	log.Fatal("Failed to register worker")
	return -1
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, dest string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", dest)
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

func (f *Files) server() {
	rpc.Register(f)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", myDest)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}
