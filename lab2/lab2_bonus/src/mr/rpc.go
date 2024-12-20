/*
 * @Author: amamiya-yuuko-1225 1913250675@qq.com
 * @Date: 2024-12-02 13:01:36
 * @LastEditors: amamiya-yuuko-1225 1913250675@qq.com
 * @Description:
 */
package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

const (
	MapFinished = iota
	ReduceFinished
	Idle
	MapFailed
	ReduceFailed
)

type RegisterArgs struct {
	WorkerId   int
	WorkerDest string
}

type RegisterReply struct {
	WorkerId int
}

type HeartRequest struct {
	WorkerId int
}

type HeartReply struct {
}

type TaskRequest struct {
	WorkerState int
	WorkerId    int
	FileName    string
	ReduceId    int
	MapId       int // add MapId
}

type TaskResponse struct {
	TaskType      string
	FileName      string
	ReduceId      int
	MapId         int
	MapCount      int
	NReduce       int
	AllDone       bool
	Data          []byte // add Data
	MapOutputDest map[int]string
}

// Add your RPC definitions here.
type SendReduceOutoutArgs struct {
	ReduceId int
	Data     map[string]string
}

type SendReduceOutputReply struct {
	Success bool
}

type FetchReduceInputArgs struct {
	ReduceId int
	MapId    int
}

type FetchReduceInputReply struct {
	Data []KeyValue
}

type GetOriginalFileArgs struct {
	Filename string
}

type GetOriginalFileReply struct {
	Data []byte
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// func coordinatorSock() string {
// 	s := "/var/tmp/5840-mr-"
// 	s += strconv.Itoa(os.Getuid())
// 	return s
// }
