package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var NMAP int = 0
var NREDUCE int = 0

func (job *MapJob) DoJob(mapf func(string, string) []KeyValue) bool {
	//clean up the old job files
	//delete all files named in format of mr-<MapJob.JobID>-* (Regex), where * means the number in [0, NREDUCE)
	//then create NReduce bucket intermediate files
	ofiles := make([]*os.File, NREDUCE)
	for i := 0; i < NREDUCE; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", job.MapJobID, i)
		os.Remove(fileName)
		ofiles[i], _ = os.Create(fileName)
	}
	// call mapf, generate a big kva map for the input txt file
	inputFile, err := os.Open(job.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", job.FileName)
	}
	content, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", job.FileName)
	}
	inputFile.Close()
	kva := mapf(job.FileName, string(content))
	// split the kva into NReduce buckets, NReduce kv pairs, and write them to the NReduce intermediate files in JSON format
	//make NReduce kv encoder for NReduce intermediate files
	encs := make([]json.Encoder, NREDUCE)
	for i := 0; i < NREDUCE; i++ {
		encs[i] = json.NewEncoder(ofiles[i])
	}
	for _, kv := range kva {
		// use ihash(key) % NRedue to select bucket
		bucket := ihash(kv.Key) % NREDUCE
		// write the kv pair to the bucket file
		if err := encs[bucket].Encode(&kv); err != nil {
			log.Fatalf("cannot write %v", kv)
		}
	}
	//write all kvs in the ofiles, then close all ofiles
	for i := 0; i < NREDUCE; i++ {
		ofiles[i].Close()
	}
	return true
}
func (job *ReduceJob) DoJob(reducef func(string, []string) string) bool {
	//clean up the old output files, mr-out-<ReduceJob.JobID>, and create a new one
	outputFileName := fmt.Sprintf("mr-out-%d", job.ReduceJobID)
	os.Remove(outputFileName)
	ofile, _ := os.Create(outputFileName)
	// read all the intermediate files, and group them by key
	// the intermediate file name is mr-<MapJob.JobID>-<ReduceJob.JobID>, just read mr-*-<ReduceJob.JobID>
	// the intermediate file format is <key> <value>, merge all intermediate files into a big kva map, and group them by key
	kva := make(map[string][]string)
	for i := 0; i < NMAP; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, job.ReduceJobID)
		inputFile, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
		inputFile.Close()

	}
	//Now kva is a map which k = words in all txt files, v = N repeated "1" string array (N is the number of times that the word appears in all txt files)
	//call reducef, and write the result to the output file
	for k, v := range kva {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	ofile.Close()
	return true
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var oldJob GetJobArg
	oldJob.JobID = -1 //-1 means no job
	oldJob.JobType = JobTypeNoJob
	CoordinatorDisconnectCnt := 0 // count the number of times that the coordinator disconnect
	for {
		// ask the coordinator for a job
		var newJob GetJobReply
		newJob.JobType = JobTypeNoJob
		ok := call("Coordinator.GetJob", &oldJob, &newJob)
		if ok {
			//reset coordinator state
			CoordinatorDisconnectCnt = 0
			//set total map job
			NMAP = newJob.TotalMapJob
			NREDUCE = newJob.TotalReduceJob
			//reset oldjob parms
			oldJob.JobID = -1 //-1 means no job
			oldJob.JobType = JobTypeNoJob
			// if no job, sleep for 1 sec, then check again (using continue)
			if newJob.JobType == JobTypeNoJob {
				time.Sleep(time.Second * 2)
				continue
			} else if newJob.JobType == JobTypeMap {
				//do map job
				mapJob := newJob.AMapJob
				if ok := mapJob.DoJob(mapf); ok {
					//set oldjob parms
					oldJob.JobID = newJob.AMapJob.MapJobID
					oldJob.JobType = newJob.JobType
				}

			} else if newJob.JobType == JobTypeReduce {
				//do reduce job
				reduceJob := newJob.AReduceJob
				if ok := reduceJob.DoJob(reducef); ok {
					//set oldjob parms
					oldJob.JobID = newJob.AReduceJob.ReduceJobID
					oldJob.JobType = newJob.JobType
				}
			}
		} else {
			// if coordinator donot respond to the request twice, exit the program
			CoordinatorDisconnectCnt++
			if CoordinatorDisconnectCnt > 1 {
				log.Fatal("Coordinator fail")
				return
			}
			//retry 1 sec later
			time.Sleep(time.Second)
			continue
		}

	}

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
