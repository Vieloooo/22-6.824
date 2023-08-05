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

// Coordinator stage list
const (
	CoorStateIDLE   = "idle"
	CoorStateMAP    = "map"
	CoorStateREDUCE = "reduce"
	CoorStateDONE   = "done"
)

// job state
const (
	JobStateUnassigned = "unassigned"
	JobStateProcessing = "processing"
	JobStateDone       = "done"
)

type Coordinator struct {

	//global vars
	NMAP          int
	NREDUCE       int
	Locker        sync.Mutex       //mutex lock which locking fields bellow
	State         string           //coordinator state: idle, map, reduce, done
	MapJobList    []MapJobState    //mapjoblist
	ReduceJobList []ReduceJobState //reducejoblist

}

type MapJobState struct {
	StartTime time.Time
	JobID     int
	FileName  string
	State     string
}
type ReduceJobState struct {
	StartTime time.Time
	JobID     int
	State     string
}

// get unsigned map job
func (c *Coordinator) getUnassignedMapJob() (MapJob, bool) {
	remaining := c.NMAP
	for i, job := range c.MapJobList {
		if job.State == JobStateUnassigned {
			c.MapJobList[i].State = JobStateProcessing
			c.MapJobList[i].StartTime = time.Now()
			tmpJob := MapJob{
				MapJobID: job.JobID,
				FileName: job.FileName,
			}
			return tmpJob, true
		} else if job.State == JobStateProcessing {
			//check if the job is timeou (10s)
			if time.Now().Sub(job.StartTime) > 10*time.Second {
				c.MapJobList[i].StartTime = time.Now()
				tmpJob := MapJob{
					MapJobID: job.JobID,
					FileName: job.FileName,
				}
				return tmpJob, true
			}
		} else if job.State == JobStateDone {
			remaining--
		}

	}
	if remaining == 0 {
		c.State = CoorStateREDUCE
	}
	return MapJob{}, false
}

// get unsigned reduce job

func (c *Coordinator) getUnassignedReduceJob() (ReduceJob, bool) {
	remaining := c.NREDUCE
	for i, job := range c.ReduceJobList {
		if job.State == JobStateUnassigned {
			c.ReduceJobList[i].State = JobStateProcessing
			c.ReduceJobList[i].StartTime = time.Now()
			tmpJob := ReduceJob{
				ReduceJobID: job.JobID,
			}
			return tmpJob, true
		} else if job.State == JobStateProcessing {
			//check if the job is timeou (10s)
			if time.Now().Sub(job.StartTime) > 10*time.Second {
				c.ReduceJobList[i].StartTime = time.Now()
				tmpJob := ReduceJob{
					ReduceJobID: job.JobID,
				}
				return tmpJob, true
			}
		} else if job.State == JobStateDone {
			remaining--
		}

	}
	if remaining == 0 {
		c.State = CoorStateDONE
	}
	return ReduceJob{}, false
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// only one new RPC is registered for the mapreduce work
// get a job from coordinator
func (c *Coordinator) GetJob(args *GetJobArg, reply *GetJobReply) error {
	//log.Println("GetJob called", *args)
	// Your code here.
	reply.TotalMapJob = c.NMAP
	reply.TotalReduceJob = c.NREDUCE
	reply.JobType = JobTypeNoJob
	c.Locker.Lock()
	defer c.Locker.Unlock()

	//commit old job state
	if args.JobType == JobTypeMap && c.MapJobList[args.JobID].State == JobStateProcessing {
		c.MapJobList[args.JobID].State = JobStateDone
	} else if args.JobType == JobTypeReduce && c.ReduceJobList[args.JobID].State == JobStateProcessing {
		c.ReduceJobList[args.JobID].State = JobStateDone
	}
	//check current state
	if c.State == CoorStateMAP {
		//log.Println("try to assigned a MAP job")
		newMapJob, ok := c.getUnassignedMapJob()
		if ok {
			//log.Println("assigned a MAP job", newMapJob)
			reply.JobType = JobTypeMap
			reply.AMapJob = newMapJob
			return nil
		}
	} else if c.State == CoorStateREDUCE {
		//log.Println("try to assigned a REDUCE job")
		newReduceJob, ok := c.getUnassignedReduceJob()
		if ok {
			//log.Println("assigned a REDUCE job", newReduceJob)
			reply.JobType = JobTypeReduce
			reply.AReduceJob = newReduceJob
			return nil
		}

	} else if c.State == CoorStateDONE {
		reply.JobType = JobTypeAllDone
		return nil
	}
	return nil
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
		//log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Locker.Lock()
	defer c.Locker.Unlock()
	if c.State == CoorStateDONE {
		return true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// main thread of coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//init coordinator state
	c.State = CoorStateIDLE
	c.NMAP = len(files)
	c.NREDUCE = nReduce
	//init map job list
	for i := 0; i < c.NMAP; i++ {
		c.MapJobList = append(c.MapJobList, MapJobState{JobID: i, FileName: files[i], State: JobStateUnassigned})
	}
	//init reduce job list
	for i := 0; i < c.NREDUCE; i++ {
		c.ReduceJobList = append(c.ReduceJobList, ReduceJobState{JobID: i, State: JobStateUnassigned})
	}
	//set coordinator state to map
	c.State = CoorStateMAP
	//start coordinator server
	c.server()
	log.Printf("Coordinator start\n")
	return &c
}
