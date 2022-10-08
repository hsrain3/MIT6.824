package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % nReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		//time.Sleep(1 * time.Second)
		reply := GetTaskReply{}
		if !call("Coordinator.RequestTask", &GetTaskArgs{}, &reply) {
			break
		}
		switch reply.TaskState {
		case Map:
			handleMap(&reply.Task, mapf)
		case Reduce:
			handleReduce(&reply.Task, reducef)
		case Wait:
			time.Sleep(1 * time.Second)
		case Done:
			fmt.Println("All task done")
			return
		}
		taskDoneArgs := reply.Task
		//notify task done
		call("Coordinator.TaskDone", &taskDoneArgs, &reply)
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func handleMap(task *Task, mapf func(string, string) []KeyValue) {
	//reduce - intermediate
	intermediate := make([][]KeyValue, task.NReduceTasks)
	for i := range intermediate {
		intermediate[i] = make([]KeyValue, 0)
	}
	file, err := os.Open(task.MapFile)
	if err != nil {
		log.Fatalf("cannot open map file %v", task.MapFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot open map file %v", task.MapFile)
	}
	file.Close()
	kva := mapf(task.MapFile, string(content))
	//ihash maps what key to which reduce task
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%task.NReduceTasks] = append(intermediate[ihash(kv.Key)%task.NReduceTasks], kv)
	}
	//write to json file
	for i := range intermediate {
		if len(intermediate[i]) == 0 {
			continue
		}
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskID, i) //map no- reduce -no
		ofile, _ := ioutil.TempFile("./", "tmp_")
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Json encode error at key-%v value-%v", kv.Key, kv.Value)
			}
		}

		ofile.Close()
		os.Rename(ofile.Name(), fileName)
	}
}

func handleReduce(task *Task, reducef func(string, []string) string) {
	log.Println("reduce worker get task ", task.TaskID)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < task.NMapTasks; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
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
	ofilename := fmt.Sprintf("mr-out-%d", task.TaskID)
	ofile, _ := ioutil.TempFile("./", "tmp_") //incase partially files in the presence of crashes

	for i := 0; i < len(intermediate); {
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
	os.Rename(ofile.Name(), ofilename)

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
