package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskInfo struct {
	TaskType string // "map" or "reduce"
	Value    string // Could be status: UnAllocated, Allocated,
	//Finishied; or filename for map; or "" for reduce
	ID int
}

type MapTask struct {
	MapId    int
	FileName string
}

const (
	UnAllocated = iota
	Allocated
	Finished
)

const (
	// heartbeat timeout in seconds
	HeartBeatTimeout = 10
)

type Coordinator struct {
	mapState         map[int]int        // state of map[int]
	reduceState      map[int]int        // state of reduce[id]
	nReduce          int                // number of reduce as defined in the paper
	reduceFinished   bool               // indicates if all reduce tasks done
	workerHeartbeats map[int]*time.Time // store last heartbeat time for workers
	workerTasks      map[int]TaskInfo   // store worker task info
	workerCounter    int                // count the number of workers
	mapCount         int                // map task count, equals number of files
	mutex            sync.Mutex         //atomicity for coordinator access
	workerDest       map[int]string     //map workerid to ip:port
	mapFileDest      map[int]int        //map mapID to workerID where stores the map output
	mapId2File       map[int]string     //map mapID to filename
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8888")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.reduceFinished
}

/**
 * @description: Worker done reduce, and send final output to coordinator
 * @param {*SendReduceOutoutArgs} args
 * @param {*SendReduceOutputReply} reply
 * @return {*}
 */
func (c *Coordinator) SendReduceOutput2Coordinator(args *SendReduceOutoutArgs, reply *SendReduceOutputReply) error {
	reduceId, output := args.ReduceId, args.Data
	// open file
	oname := fmt.Sprintf("mr-out-%d.txt", reduceId)
	ofile, _ := os.Create(oname)
	for k, v := range output {
		fmt.Fprintf(ofile, "%v %v\n", k, v)
	}
	ofile.Close()
	log.Printf("Final output %d delivered", reduceId)
	return nil
}

func (c *Coordinator) GetOriginalFile(args *GetOriginalFileArgs, reply *GetOriginalFileReply) error {
	filename := args.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	reply.Data = content
	log.Printf("original file %s delivered", filename)
	return nil
}

/**
 * @description: worker call this via RPC to acquire task/report finished task
 * @param {*TaskRequest} args
 * @param {*TaskResponse} reply
 * @return {*}
 */
func (c *Coordinator) AllocateTasks(args *TaskRequest, reply *TaskResponse) error {
	workerId := args.WorkerId
	reply.NReduce = c.nReduce
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// if all done, notify worker to exit
	if c.reduceFinished {
		reply.AllDone = true
		return nil
	}
	// if worker is idle
	if args.WorkerState == Idle {
		mapFinished := true
		for mapId, state := range c.mapState {
			if state != Finished {
				mapFinished = false
			}
			if state == UnAllocated {
				filename := c.mapId2File[mapId]
				// change state of the map task
				c.mapState[mapId] = Allocated
				// fill in reply to the worker
				reply.MapId = mapId + 1
				reply.TaskType = "map"
				reply.FileName = filename
				reply.NReduce = c.nReduce
				//record worker task in coordinator
				c.workerTasks[workerId] = TaskInfo{"map", filename, mapId}
				return nil
			}
		}
		for reduceId, state := range c.reduceState {
			if !mapFinished {
				break
			}
			if state == UnAllocated {
				c.reduceState[reduceId] = Allocated
				reply.TaskType = "reduce"
				reply.ReduceId = reduceId + 1
				reply.MapCount = c.mapCount
				// notify the reduce worker with all the locations of
				// intermediate files
				mapOutputDest := make(map[int]string)
				reply.MapOutputDest = mapOutputDest
				for mi, wi := range c.mapFileDest {
					mapOutputDest[mi] = c.workerDest[wi]
				}
				c.workerTasks[workerId] = TaskInfo{"reduce", "", reduceId}
				return nil
			}
		}
	} else if args.WorkerState == MapFinished {
		// label the specific map task as done
		c.mapState[args.MapId] = Finished
		// mark that the ip:port of workerID is avaliable to get mapID output files
		c.mapFileDest[args.MapId] = workerId
		log.Printf("map dest %d %d", args.MapId, args.WorkerId)
		// change worker to "idle", stop checking heartbeat
		reply.TaskType = "idle"
		delete(c.workerTasks, workerId) // delete worker task record
	} else if args.WorkerState == ReduceFinished {
		c.reduceState[args.ReduceId] = Finished
		if checkReduceTaskAllDone(c) {
			c.reduceFinished = true
		}
		reply.TaskType = "idle"
		delete(c.workerTasks, workerId) // delete worker task record
	} else if args.WorkerState == MapFailed {
		c.reallocateMap(args.MapId)
		reply.TaskType = "idle"
	} else if args.WorkerState == ReduceFailed {
		c.reallocateReduce(args.ReduceId)
		reply.TaskType = "idle"
	}
	return nil
}

/**
 * @description: Check heartbeat of worker. If a worker is dead (not responded in 10s)
 * 					then reallocate the related tasks
 * @param {int} workerId
 * @return {*}
 */
func (c *Coordinator) checkHeartBeat(workerId int) {
	for {
		//check heartbeat for every interval
		time.Sleep(200 * time.Millisecond)
		c.mutex.Lock()
		now := time.Now()
		if lastHeartbeatPointer, ok := c.workerHeartbeats[workerId]; ok {
			// for those already deemed dead
			if lastHeartbeatPointer == nil {
				c.mutex.Unlock()
				continue
			}
			lastHeartbeat := *lastHeartbeatPointer
			// if last heartbeat is older than 10s:
			if now.Sub(lastHeartbeat) > HeartBeatTimeout*time.Second {
				c.workerHeartbeats[workerId] = nil
				// reallocate its executing map task
				if taskInfo, ok := c.workerTasks[workerId]; ok {
					if taskInfo.TaskType == "map" && c.mapState[taskInfo.ID] != Finished {
						c.mapState[taskInfo.ID] = UnAllocated
					}
				}
				log.Printf("%d", workerId)

				hasDoneSomeMap := false
				// reallocate all map tasks done on it
				for mi, wi := range c.mapFileDest {
					if wi == workerId {
						c.reallocateMap(mi)
						hasDoneSomeMap = true
					}
				}
				// reallocate all ongoing (allocated but unfinished) reduce tasks.
				// since ANY reduce task are done based on EVERY map output
				if hasDoneSomeMap {
					for ri := 0; ri < c.nReduce; ri++ {
						if c.reduceState[ri] == Allocated {
							c.reallocateReduce(ri)
						}
					}
				}

				delete(c.workerTasks, workerId) // delete worker task record
			}
		}
		c.mutex.Unlock()
	}

}

func (c *Coordinator) reallocateMap(mapId int) {
	log.Printf("reallocate map %d", mapId)
	c.mapState[mapId] = UnAllocated
	c.mapFileDest[mapId] = -1
}

func (c *Coordinator) reallocateReduce(reduceId int) {
	log.Printf("reallocate reduce %d", reduceId)
	c.reduceState[reduceId] = UnAllocated
	c.reduceFinished = false
}

func (c *Coordinator) ReceiveHeartbeat(arg *HeartRequest, reply *HeartReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	now := time.Now()
	id := arg.WorkerId
	c.workerHeartbeats[id] = &now
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.workerCounter++
	workerId := c.workerCounter
	reply.WorkerId = workerId
	now := time.Now()
	c.workerHeartbeats[workerId] = &now
	c.workerDest[workerId] = args.WorkerDest
	go c.checkHeartBeat(workerId)
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapState:         make(map[int]int),
		reduceState:      make(map[int]int),
		workerHeartbeats: make(map[int]*time.Time),
		nReduce:          nReduce,
		workerTasks:      make(map[int]TaskInfo),
		reduceFinished:   false,
		mutex:            sync.Mutex{},
		workerDest:       make(map[int]string),
		mapFileDest:      make(map[int]int),
		mapId2File:       make(map[int]string),
	}
	// add files, create related map tasks
	for i, filename := range files {
		mapId := i
		c.mapCount++
		c.mapState[mapId] = UnAllocated
		c.mapId2File[mapId] = filename
		c.mapFileDest[mapId] = -1
	}
	// create reducer channel given NReduced
	for i := 0; i < nReduce; i++ {
		c.reduceState[i] = UnAllocated
	}
	c.server()
	return &c
}

// iterate all map tasks, chekc if all done
func checkMapTaskAllDone(c *Coordinator) bool {
	for _, state := range c.mapState {
		if state != Finished {
			return false
		}
	}
	return true
}

// iterate all reduce tasks, check if all done
func checkReduceTaskAllDone(c *Coordinator) bool {
	for _, state := range c.reduceState {
		if state != Finished {
			return false
		}
	}
	return true
}
