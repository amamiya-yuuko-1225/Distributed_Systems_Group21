package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
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
	coordinatorDest = "localhost:8888"
)

var myDest string

type Files struct {
}

/**
 * @description: read map output file, and return kv
 * @param {string} name
 * @return {*} kv pairs
 */
func readIntermediateFile(name string) []KeyValue {
	if !fileExists(name) {
		return nil
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
	return intermediate
}

// Other workers call this by RPC to get file on this machine
func (f *Files) GetintermediateFile(arg *FetchReduceInputArgs, reply *FetchReduceInputReply) error {
	name := fmt.Sprintf("mr-%d-%d", arg.MapId, arg.ReduceId)
	intermediate := readIntermediateFile(name)
	if intermediate == nil {
		return errors.New("get intermediate file failed")
	}
	reply.Data = intermediate
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string, ip string, port string) {
	//register worker rpc server to transmit intermediate files
	myDest = ip + ":" + port
	f := Files{}
	f.server(port)

	workerId := registerWorker()
	// send heartbeat to coordinator
	go sendHeartbeat(workerId)
	var args TaskRequest
	reply := TaskResponse{}
	reply.TaskType = "idle"
	// ask coordinator for a task
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
			if execMap(reply.FileName, mapf, reply.MapId, reply.NReduce) {
				args = TaskRequest{WorkerState: MapFinished, WorkerId: workerId, FileName: reply.FileName, MapId: reply.MapId}
			} else {
				args = TaskRequest{WorkerState: MapFailed, WorkerId: workerId, FileName: reply.FileName, MapId: reply.MapId}
			}
		} else {
			if execReduce(reply.ReduceId, reducef, reply.MapCount, reply.MapOutputDest) {
				args = TaskRequest{WorkerState: ReduceFinished, WorkerId: workerId, ReduceId: reply.ReduceId}
			} else {
				args = TaskRequest{WorkerState: ReduceFailed, WorkerId: workerId, ReduceId: reply.ReduceId}
			}
		}
		ok := call("Coordinator.AllocateTasks", coordinatorDest, &args, &reply)
		// Coordinator failure deteced through the return value of RPC
		if !ok {
			log.Fatal("Coordinator failure: allocate task failed, exiting...")
		}
	}
}

func execMap(filename string, mapf func(string, string) []KeyValue, mapId int, nReduce int) bool {
	log.Printf("map %d starts", mapId)

	// Get original file from coordinator
	args := GetOriginalFileArgs{
		Filename: filename,
	}
	reply := GetOriginalFileReply{}
	ok := call("Coordinator.GetOriginalFile", coordinatorDest, &args, &reply)
	if !ok {
		log.Fatalf("get original file %s failed, coordinator failure...", filename)
	}
	// do map task
	kvs := mapf(filename, string(reply.Data))

	//create intermediate JSON files
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// create temp files, in order to prevent corrupt data being fetched
		nameTemp := fmt.Sprintf("mr-%d-%d_", mapId, i)
		var err error
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
	return true
}

func execReduce(reduceId int, reducef func(string, []string) string, nMap int, dests map[int]string) bool {
	log.Printf("reduce %d starts", reduceId)
	// fetch intermediate file from workers through RPC
	// iterate ALL workers who have done "map" to fetch the needed file
	intermediate := []KeyValue{}
	for i := 1; i <= nMap; i++ {
		args := FetchReduceInputArgs{
			ReduceId: reduceId,
			MapId:    i,
		}
		reply := FetchReduceInputReply{}
		values := []KeyValue{}
		if dests[i] == myDest {
			values = readIntermediateFile(fmt.Sprintf("mr-%d-%d", i, reduceId))
		} else {
			log.Printf("%s %s", myDest, dests[i])
			ok := call("Files.GetintermediateFile", dests[i], &args, &reply)
			if !ok {
				log.Printf("Fetch intermediate files failure: mr-%d-%d, abort task", i, reduceId)
				return false
			}
			values = reply.Data
		}
		intermediate = append(intermediate, values...)

	}

	// sort by key
	sort.Sort(ByKey(intermediate))

	finalResult := map[string]string{}

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
		finalResult[intermediate[i].Key] = output
		i = j
	}

	//send final result to coordinator
	args := SendReduceOutoutArgs{
		ReduceId: reduceId,
		Data:     finalResult,
	}
	reply := SendReduceOutputReply{}
	ok := call("Coordinator.SendReduceOutput2Coordinator", coordinatorDest, &args, &reply)
	if !ok {
		log.Printf("Send reduce %d output to master failed. Abort task", reduceId)
		return false
	}
	log.Printf("reduce %d done", reduceId)
	return true
}

// send hearbeat
func sendHeartbeat(workerId int) {
	for {
		time.Sleep(200 * time.Millisecond)
		args := HeartRequest{WorkerId: workerId}
		reply := HeartReply{}
		ok := call("Coordinator.ReceiveHeartbeat", coordinatorDest, &args, &reply)
		if !ok {
			log.Fatal("Coordinator failure: allocate task failed, exiting...")
		}
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
	// we have timeout here
	// because while worker A getting files from worker B
	// the worker B may suddenly dies
	// then worker A should exit and request task reallocation
	timeout := time.Duration(50 * time.Millisecond)
	done := make(chan error, 1)
	c, err := rpc.DialHTTP("tcp", dest)
	if err != nil {
		log.Print("dialing:", err)
		return false
	}
	defer c.Close()
	go func() {
		err = c.Call(rpcname, args, reply)
		done <- err
	}()
	select {
	case <-time.After(timeout):
		log.Printf("rpc call timeout =>%v", dest)
		return false
	case err := <-done:
		if err != nil {
			return false
		}
	}
	return true
}

func (f *Files) server(port string) {
	rpc.Register(f)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "0.0.0.0:"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}
