package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type WorkerSlot struct {
	idle     bool
	complete bool
	workerID int
}

type MapTask struct {
	taskId   int
	filename string
	// 0 will be not started
	// 1 running
	// 2 done
	stage   int
	fileCh  chan []string //channel waiting on signal with files from worker
	started time.Time
}

type ReduceTask struct {
	taskId     int
	taskBucket int
	// 0 will be not started
	// 1 running
	// 2 done
	stage  int
	doneCh chan int //channel waiting on signal from reduce worker done

	started time.Time
}

type Coordinator struct {
	// Your definitions here.
	mapJobs        []MapTask
	mapJobsDone    int
	reduceJobs     []ReduceTask
	reduceJobsDone int
	nReduce        int
	workers        map[int]WorkerSlot
	workerInc      int
	mu             sync.Mutex
}

// Go routine which will run after a worker has completed a map task, waiting on coordinator to ack
func (c *Coordinator) waitForWorkerCompletion(reply *TaskReply, jobID int) {
	if reply.TaskType == reduceTask {
		fmt.Printf("Waiting for worker completion on reducejob %v, going to loop 10s\n", reply.ReduceJobID)
	} else {
		fmt.Printf("Waiting for worker completion on mapjob %v, going to loop 10s\n", reply.MapJobID)
	}

	for start := time.Now(); time.Since(start) < (time.Second * time.Duration(10)); {
		if reply.TaskType == mapTask {
			select {
			case intFilesFromWorker := <-c.mapJobs[jobID].fileCh:
				fmt.Println("recieved intFiles from map worker, job done!")
				fmt.Println(intFilesFromWorker[0])
				return
			default:
			}
		} else {
			select {
			case reduceJobID := <-c.reduceJobs[reply.ReduceJobID].doneCh:
				fmt.Println("recieved ack from reduce job done!" + strconv.Itoa(reduceJobID))
				return
			default:
			}
		}
	}
	//Worker is taking too long! Reset task
	fmt.Println("Worker timeout on " + reply.Filename + strconv.Itoa(reply.ReduceJobID))
	c.mu.Lock()
	if reply.TaskType == mapTask {
		fmt.Printf(".. Worker mapJobID was %v\n", reply.MapJobID)
		c.mapJobs[jobID].stage = taskNotStarted
	} else {
		fmt.Printf(".. Worker reduceJobID was %v, unassigning task..\n", reply.ReduceJobID)
		c.reduceJobs[jobID].stage = taskNotStarted
	}
	c.mu.Unlock()

}

//RPC method called from worker to notify map completion
func (c *Coordinator) NotifyMapOpComplete(args *NotifyMapDoneRequest, reply *NotifyMapDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//a worker has signalled completion of a map f,
	mapJobID := args.MapJobID
	// update the mapJobID to done
	c.mapJobs[mapJobID].stage = taskCompleted //mark job as done
	c.mapJobsDone += 1
	fileCh := c.mapJobs[mapJobID].fileCh
	fileCh <- args.IntFileNames
	return nil
}

//RPC method called from worker to notify reduce completion
func (c *Coordinator) NotifyReduceOpComplete(args *NotifyReduceDoneRequest, reply *NotifyReduceDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//a worker has signalled completion of a reduce f,
	reduceJobID := args.ReduceJobID
	// update the reduceJobID to done
	c.reduceJobs[reduceJobID].stage = taskCompleted //mark job as done
	c.reduceJobsDone += 1
	doneCh := c.reduceJobs[reduceJobID].doneCh

	fmt.Printf("retrieved doneCh %v and writing reduceopComplete..\n", strconv.Itoa(reduceJobID))
	doneCh <- reduceJobID
	return nil
}

// RPC handler for worker getTask call.
// the RPC argument and reply types are defined in rpc.go.
// Go runs the handler for each RPC in its own thread, so the fact that one handler is
// waiting won't prevent the coordinator from processing other RPCs.
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	//pick what kind of task the worker gets
	if c.mapJobsDone != len(c.mapJobs) { //still mappin`!
		fmt.Println("map time!")
		for i, mapJob := range c.mapJobs {
			if mapJob.stage == taskNotStarted {
				reply.Filename = mapJob.filename
				reply.MapJobID = i
				reply.Reducers = c.nReduce
				reply.TaskType = mapTask
				c.mapJobs[i].stage = taskInProgress
				c.mapJobs[i].started = time.Now()
				go c.waitForWorkerCompletion(reply, i)
				return nil
			}
		}
		//getting to here means no map jobs open
		//lets tell the worker to wait
		reply.TaskType = waitTask
	} else if c.reduceJobsDone != len(c.reduceJobs) && c.mapJobsDone == len(c.mapJobs) {
		fmt.Println("Reduce time!")
		for i, reduceJob := range c.reduceJobs {
			if reduceJob.stage == taskNotStarted {
				reply.Reducers = c.nReduce
				reply.TaskType = reduceTask
				reply.ReduceJobID = i
				reply.BucketID = reduceJob.taskBucket
				c.reduceJobs[i].stage = taskInProgress
				c.reduceJobs[i].started = time.Now()
				//TODO : wait on the reduce worker for completion
				go c.waitForWorkerCompletion(reply, i)
				return nil
			}
		}
		//getting to here means no reduce jobs open
		//lets tell the worker to wait
		reply.TaskType = waitTask
	}

	if c.mapJobsDone == len(c.mapJobs) && c.reduceJobsDone == len(c.reduceJobs) {
		reply.TaskType = -1 //tell worker its time for retirement
	} else {
		reply.TaskType = waitTask //tell worker to wait for map stragglers
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	if c.mapJobsDone == len(c.mapJobs) && c.reduceJobsDone == len(c.reduceJobs) {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var map_jobs []MapTask
	var reduce_jobs []ReduceTask
	for i, file := range files {
		map_jobs = append(map_jobs, MapTask{taskId: i,
			filename: file, stage: 0, fileCh: make(chan []string)})
	}
	for i := 0; i < nReduce; i++ {
		reduce_jobs = append(reduce_jobs, ReduceTask{taskId: i,
			taskBucket: i, stage: 0, doneCh: make(chan int)})
	}
	map_jobs_done := 0
	c := Coordinator{map_jobs, map_jobs_done, reduce_jobs, 0, nReduce,
		make(map[int]WorkerSlot), 0, sync.Mutex{}}
	// Your code here.

	c.server()
	return &c
}
