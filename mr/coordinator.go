package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	TaskId        int
	FileName      []string
	NMaps         int
	NReduce       int
	Phase         string
	MapChannel    chan *Task
	ReduceChannel chan *Task
	TaskInfoSets  []*Task
	mu            sync.Mutex
}

func (c *Coordinator) CallForTask(args *Args, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	c.CheckLostWorker()

	var taskreceived *Task
	if c.Phase == "Map" {
		select {
		case taskreceived = <-c.MapChannel:
			{
				taskreceived.StartTime = time.Now()
				*reply = *taskreceived
				fmt.Print("Task sent:")
				fmt.Println(reply)
			}
		default:
			{
				fmt.Println("No more Task")
				reply.TaskTyge = "Wait"

				changeflag := true
				for i := 0; i < len(c.FileName); i++ {
					if c.TaskInfoSets[i].TaskTyge != "Wait" {
						changeflag = false
					}
				}
				if changeflag {
					c.mu.Lock()
					c.Phase = "Reduce"
					c.mu.Unlock()
				}
			}
		}
	} else if c.Phase == "Reduce" {
		select {
		case taskreceived = <-c.ReduceChannel:
			{
				taskreceived.StartTime = time.Now()
				*reply = *taskreceived
				fmt.Print("Task sent:")
				fmt.Println(reply)
			}
		default:
			{
				fmt.Println("No more Task")
				reply.TaskTyge = "Wait"

				changeflag := true
				for i := c.NMaps; i < c.NMaps+c.NReduce; i++ {
					if c.TaskInfoSets[i].TaskTyge != "Wait" {
						changeflag = false
					}
				}
				if changeflag {
					c.mu.Lock()
					c.Phase = "AllDone"
					c.mu.Unlock()
				}
			}
		}
	} else {
		reply.TaskTyge = "Kill"
	}
	return nil
}

func (c *Coordinator) CheckLostWorker() {

	for i := 0; i < c.NMaps+c.NReduce; i++ {
		if !c.TaskInfoSets[i].StartTime.IsZero() && c.TaskInfoSets[i].TaskTyge != "Wait" {
			if time.Since(c.TaskInfoSets[i].StartTime) > time.Second*10 {
				c.TaskInfoSets[i].StartTime = time.Time{}
				if c.TaskInfoSets[i].TaskTyge == "Map" {
					c.MapChannel <- c.TaskInfoSets[i]
				} else if c.TaskInfoSets[i].TaskTyge == "Reduce" {
					c.ReduceChannel <- c.TaskInfoSets[i]
				}
				fmt.Printf("Task reload:%+v", c.TaskInfoSets[i])
			}
		}
	}
}

func (c *Coordinator) CallForMissionDone(args *Args, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if "Map" == c.TaskInfoSets[args.TaskId].TaskTyge {
		fmt.Printf("Task Map %v have finished\n", args.TaskId)
		c.TaskInfoSets[args.TaskId].TaskTyge = "Wait"

	} else if "Reduce" == c.TaskInfoSets[args.TaskId].TaskTyge {
		fmt.Printf("Task Reduce %v have finished\n", args.TaskId)
		c.TaskInfoSets[args.TaskId].TaskTyge = "Wait"

	} else {
		fmt.Println("Unknown state")
		fmt.Println(c.TaskInfoSets[args.TaskId])

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
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Phase == "AllDone" {
		return true
	}

	return false

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var c Coordinator
	CoordinatorInit(files, nReduce, &c)
	fmt.Println("CoordinatorInit!")
	TaskInit(&c)
	fmt.Println("TaskInit!")
	for _, i := range c.TaskInfoSets {
		fmt.Println(i)
	}
	// Your code here.

	c.server()
	return &c
}

func CoordinatorInit(files []string, nReduce int, c *Coordinator) bool {
	c.NReduce = nReduce
	c.TaskId = 0
	c.Phase = "Map"
	c.NMaps = len(files)
	c.MapChannel = make(chan *Task, len(files))
	c.ReduceChannel = make(chan *Task, nReduce)
	c.FileName = files
	c.TaskInfoSets = make([]*Task, len(files)+nReduce)
	return true
}

func TaskInit(c *Coordinator) {
	for _, file := range c.FileName {
		NewTask := Task{
			TaskTyge: "Map",
			FileName: []string{file},
			TaskId:   c.TaskId,
			NReduce:  c.NReduce,
			NMaps:    c.NMaps,
		}
		c.TaskInfoSets[NewTask.TaskId] = &NewTask
		c.TaskId++
		c.MapChannel <- &NewTask
	}

	/////////////

	for i := 0; i < c.NReduce; i++ {
		strFiles := make([]string, c.NMaps)
		for j := 0; j < c.NMaps; j++ {
			strFiles[j] = fmt.Sprintf("mr-temp-%d-%d", j, i)
		}
		NewTask := Task{
			TaskTyge: "Reduce",
			FileName: strFiles,
			TaskId:   c.TaskId,
			NReduce:  c.NReduce,
			NMaps:    c.NMaps,
		}
		c.TaskInfoSets[NewTask.TaskId] = &NewTask
		c.TaskId++
		c.ReduceChannel <- &NewTask

	}
}
