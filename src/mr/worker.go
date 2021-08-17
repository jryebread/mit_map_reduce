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
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func NotifyCoordinatorMapOpComplete(taskInfo *TaskReply, intFileNames []string) {
	mapJobID := taskInfo.MapJobID
	// declare an argument structure.
	args := NotifyMapDoneRequest{MapJobID: mapJobID, IntFileNames: intFileNames}
	reply := NotifyMapDoneResponse{}

	// send the RPC request, wait for the reply.
	res := call("Coordinator.NotifyMapOpComplete", &args, &reply)

	if !res {
		log.Fatal("Something went wrong notifying the coordinator of succ map!")
	}
}

func NotifyCoordinatorReduceOpComplete(task *TaskReply) {
	args := NotifyReduceDoneRequest{ReduceJobID: task.ReduceJobID}
	reply := NotifyReduceDoneResponse{}
	fmt.Println("Notifying coordinator of reduce op complete")
	res := call("Coordinator.NotifyReduceOpComplete", &args, &reply)
	if !res {
		log.Fatal("Something went wrong notifying the coordinator of succ reduce!")
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//Worker is alive! ask coordinator for task
	var taskTypeResponse int

	for taskTypeResponse != -1 {
		taskInfo := GetTask()
		taskTypeResponse = taskInfo.TaskType
		if taskTypeResponse == mapTask {
			//TIME TO MAP
			intFileNames, err := ExecuteMapTask(taskInfo, mapf)
			if err != nil {
				log.Fatal("Worker reporting failed map:" + err.Error())
			}
			NotifyCoordinatorMapOpComplete(taskInfo, intFileNames)
			fmt.Println("Done with map job: ", strconv.Itoa(taskInfo.MapJobID))
		} else if taskTypeResponse == reduceTask {
			//TIME TO REDUCE
			ExecuteReduceTask(taskInfo, reducef)
			NotifyCoordinatorReduceOpComplete(taskInfo)
		} else if taskTypeResponse == waitTask {
			//TIME TO wait.. wait just wait? ok.
			//worker should wait for stragglers if no jobs open to take
			fmt.Println("Worker was told to wait by coordinator..")
			time.Sleep(2 * time.Second)
		} else { //Retirement
			return //to the MEATLOAF! TO THE BROCOLLIIII https://www.youtube.com/watch?v=IP5LRwH34DY
		}
		//TODO: REDUCE TASK
	}
}

func ExecuteReduceTask(taskInfo *TaskReply, reducef func(string, []string) string) {
	bucketID := taskInfo.BucketID
	toMatch := "mr-*-" + strconv.Itoa(bucketID)
	matches, err := filepath.Glob(toMatch)
	if err != nil {
		log.Fatalf("cannot glob filepath %v %v\n", toMatch, err.Error())
	}
	intermediate := []KeyValue{}
	for _, fileName := range matches {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		//decode each file back into kv in memory
		//this is where we would have to do an external sort if 
		//the data was too large from the sum of all these files
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	ofile, err := ioutil.TempFile("../tmp", "mr-out")
	oldname := ofile.Name()
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-(bucketid).
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
	new_name := "mr-out-"+strconv.Itoa(taskInfo.BucketID)

	err = os.Rename(oldname, new_name)
	if err != nil {
		fmt.Printf("Failed to rename file %v to %v\n", oldname, new_name)
	}

}

func ExecuteMapTask(taskInfo *TaskReply, mapf func(string, string) []KeyValue) ([]string, error) {
	//read that file and call map
	file, err := os.Open(taskInfo.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", taskInfo.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskInfo.Filename)
	}
	file.Close()

	//call map on filename provided from coordinator
	kva := mapf(taskInfo.Filename, string(content))
	//kva is kv array, ex:{ a,1 , b,1 , a,1 ....

	//encode kv pair array to json to be stored intermediatedly in file
	//(reduce task will decode)
	return writeMapEncodingToFile(kva, taskInfo)

}

//create several temporary file while we write encoded values
//rename when done to real map intermediate value to prevent
// some worker observing partially written files
func writeMapEncodingToFile(kva []KeyValue, taskInfo *TaskReply) ([]string, error) {

	intermediateFiles := make(map[int]*os.File)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % taskInfo.Reducers
		file, ok := intermediateFiles[bucket]
		if !ok {
			//the file for this bucket has not been created yet
			// create a temp file to be renamed later
			tmpfile, err := ioutil.TempFile("../tmp", "mr-tmp")
			if err != nil {
				// if we fail to create a file return err
				fmt.Println("Failed to create a tmp file")
				return nil, err
			}
			//Change permission so that it can be moved
			err = os.Chmod(tmpfile.Name(), 0777)
			if err != nil {
				log.Println(err)
			}
			intermediateFiles[bucket] = tmpfile
			file = tmpfile
		}

		enc := json.NewEncoder(file)
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("Failed to Encode kv pair to file! ", file.Name())
			log.Fatal(err)
		}
	}
	var fileNames []string
	//rename all files and close file handlers
	for k, v := range intermediateFiles {
		old_file_name := v.Name()
		v.Close()

		new_file_name :=
			fmt.Sprintf("mr-%v-%v", strconv.Itoa(taskInfo.MapJobID), strconv.Itoa(k))
		err := os.Rename(old_file_name, new_file_name)
		if err != nil {
			e := err.(*os.LinkError)
			fmt.Println("Op: ", e.Op)
			fmt.Println("Old: ", e.Old)
			fmt.Println("New: ", e.New)
			fmt.Println("Err: ", e.Err)
			log.Fatalf("Failed to rename file %v to %v", old_file_name, new_file_name)
		}

		fileNames = append(fileNames, new_file_name)

	}

	return fileNames, nil
}

//
// Get task from coordinator (including fileName and taskType(reduce/map))
// the RPC argument and reply types are defined in rpc.go.
//
func GetTask() *TaskReply {

	// declare an argument structure.
	args := TaskRequest{}

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	res := call("Coordinator.GetTask", &args, &reply)
	if !res {
		fmt.Println("Something went wrong calling coordinator.GetTask()!")
		//Probably coordinator is done, so worker can return
		reply.TaskType = -1 //tell worker to exit
	}
	// reply.filename should be the filename we work on
	fmt.Printf("reply.filename %v\n", reply.Filename)
	fmt.Printf("reply.taskType %v\n", reply.TaskType)
	fmt.Printf("reply.mapJobId %v %v\n", reply.MapJobID, reply.ReduceJobID)
	return &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
