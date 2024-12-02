package mr

import (
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

type Coordinator struct {
	mapState       map[string]int   // state of map[filename]
	reduceState    map[int]int      // state of reduce[id]
	mapCh          chan MapTask     // map task channel
	reduceCh       chan int         //reduce task channel
	nReduce        int              // number of reduce as defined in the paper
	mapFinished    bool             // indicates if all map tasks done
	reduceFinished bool             // indicates if all reduce tasks done
	workerTasks    map[int]TaskInfo // store worker task info
	workerCounter  int              // count the number of workers
	mapCount       int              // map task count, equals number of files
	mutex          sync.Mutex       //atomicity for coordinator access
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.reduceFinished {
		// wait same time for propagating the finished info to all workers
		time.Sleep(1000 * time.Millisecond)
		return true
	}
	return false
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
		// if still have map tasks:
		if len(c.mapCh) > 0 {
			// get task from mapCh
			task := <-c.mapCh
			filename := task.FileName
			// change state of the map task
			c.mapState[filename] = Allocated
			// fill in reply to the worker
			reply.TaskType = "map"
			reply.FileName = filename
			reply.MapId = task.MapId
			//record worker task in coordinator
			c.workerTasks[workerId] = TaskInfo{"map", filename, task.MapId}
			// check living every 10s
			go func(mapID int, filename string, workerId int) {
				time.Sleep(10 * time.Second)
				c.mutex.Lock()
				if c.mapState[filename] != Finished {
					c.mapCh <- MapTask{mapID, filename}
					c.mapState[filename] = UnAllocated
					delete(c.workerTasks, workerId)
				}
				c.mutex.Unlock()
			}(task.MapId, filename, workerId)
		} else if len(c.reduceCh) != 0 && c.mapFinished {
			// if map is done, do reduce tasks:
			reduceId := <-c.reduceCh
			c.reduceState[reduceId] = Allocated
			reply.TaskType = "reduce"
			reply.ReduceId = reduceId
			reply.MapCount = c.mapCount
			c.workerTasks[workerId] = TaskInfo{"reduce", "", reduceId}
			go func(reduceId int, workerId int) {
				time.Sleep(10 * time.Second)
				c.mutex.Lock()
				if c.reduceState[reduceId] != Finished {
					id := reduceId
					c.reduceCh <- id
					c.reduceState[id] = UnAllocated
					delete(c.workerTasks, workerId)
				}
				c.mutex.Unlock()
			}(reduceId, workerId)
		}
	} else if args.WorkerState == MapFinished {
		// label the specific map task as done
		c.mapState[args.FileName] = Finished
		// if all map done, label mapFinished = true
		if checkMapTaskAllDone(c) {
			c.mapFinished = true
		}
		reply.TaskType = "idle"
		delete(c.workerTasks, workerId) // delete worker task record
	} else if args.WorkerState == ReduceFinished {
		c.reduceState[args.ReduceId] = Finished
		if checkReduceTaskAllDone(c) {
			c.reduceFinished = true
		}
		reply.TaskType = "idle"
		delete(c.workerTasks, workerId) // delete worker task record
	}
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.workerCounter++
	workerId := c.workerCounter
	reply.WorkerId = workerId
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapState:       make(map[string]int),
		reduceState:    make(map[int]int),
		mapCh:          make(chan MapTask, len(files)+5),
		reduceCh:       make(chan int, nReduce+5),
		nReduce:        nReduce,
		workerTasks:    make(map[int]TaskInfo),
		mapFinished:    false,
		reduceFinished: false,
		mutex:          sync.Mutex{},
	}
	// add files, create related map tasks
	for i, filename := range files {
		mapId := i
		c.mapCount++
		c.mapState[filename] = UnAllocated
		c.mapCh <- MapTask{FileName: filename, MapId: mapId}
	}
	// create reducer channel given NReduced
	for i := 0; i < nReduce; i++ {
		c.reduceState[i] = UnAllocated
		c.reduceCh <- i
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
