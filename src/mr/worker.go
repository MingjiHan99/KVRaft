package mr

import "fmt"
import "log"
import "os"
import "net/rpc"
import "hash/fnv"
import "time"
import "io/ioutil"
import "strconv"
import "sort"
import "strings"


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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	CallExample()
	err_count := 0
	// Your worker implementation here.
	for {
		taskReqArgs := TaskRequestArgs{}
		taskReqReply := TaskRequestReply{}

		err := call("Master.TaskRequest", &taskReqArgs, &taskReqReply) 
		if err == true {
			err_count = 0
			log.Printf("No Error %v", taskReqReply.TaskTypo)
			switch taskReqReply.TaskTypo {
				case TaskType_Map: {
					log.Printf("Get Map task %v nReduce %v path: %v\n", taskReqReply.TaskId, taskReqReply.NReduce, taskReqReply.MapFile)
					file, err := os.Open(taskReqReply.MapFile)
					if err != nil {
						log.Fatalf("cannot open %v", taskReqReply.MapFile)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", taskReqReply.MapFile)
					}

					file.Close()
					kva := mapf(taskReqReply.MapFile, string(content))
					// dump results into nReduce intermediate files
					interNameStart := "mr-" + strconv.Itoa(taskReqReply.TaskId) + "-"
					
					files := make([]*os.File, 0)
					
					for i := 0; i < taskReqReply.NReduce; i++ {
						file, _ := os.Create(interNameStart + strconv.Itoa(i))
						files = append(files, file)
					}

					for _, kv := range kva {
						hashValue := ihash(kv.Key) % taskReqReply.NReduce
						fmt.Fprintf(files[hashValue], "%v %v\n", 
										kv.Key, kv.Value)
					}

					for i := 0; i < taskReqReply.NReduce; i++ {
						files[i].Close()
					}

					taskCompleteArgs := TaskCompleteArgs{}
					taskCompleteArgs.TaskTypo = TaskType_Map
					taskCompleteArgs.TaskId = taskReqReply.TaskId
					taskCompleteReply := TaskCompleteReply{}
					call("Master.TaskComplete", &taskCompleteArgs, &taskCompleteReply)
			
				}
				case TaskType_Reduce: {
					log.Printf("Get Reduce task %v nMap: %v\n", taskReqReply.TaskId, taskReqReply.NMap)
					intermediate := []KeyValue{}
					for i := 0; i < taskReqReply.NMap; i++ {
						filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskReqReply.TaskId)
						file, err := os.Open(filename)
						if err != nil {
							log.Fatalf("cannot open %v", filename)
						}
						content, err := ioutil.ReadAll(file)
						if err != nil {
							log.Fatalf("cannot read %v", filename)
						}
						temp := strings.Split(string(content),"\n")
						
						for _, str := range temp {
							kvs := strings.Split(str, " ")
							if len(kvs) == 2 {
								kv := KeyValue {
									Key: kvs[0],
									Value: kvs[1] }
								intermediate = append(intermediate, kv)
							}
							
						}

						file.Close()
					
					}
					oname := "mr-out-" + strconv.Itoa(taskReqReply.TaskId)
					ofile, _ := os.Create(oname)
			
					sort.Sort(ByKey(intermediate))

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
					log.Printf("%v closed", oname)
					ofile.Close()
					taskCompleteArgs := TaskCompleteArgs{}
					taskCompleteArgs.TaskTypo = TaskType_Reduce
					taskCompleteArgs.TaskId = taskReqReply.TaskId
					taskCompleteReply := TaskCompleteReply{}
					call("Master.TaskComplete", &taskCompleteArgs, &taskCompleteReply)
				}
				case TaskType_Exit: {
					log.Printf("MapReduce task is done, Exit.")
					os.Exit(0)
				}
				case TaskType_Empty: {
					log.Printf("Get no task, Sleep.")
					time.Sleep(time.Second)
				}
			}
		} else {
			err_count += 1
		}

		if err_count < 10 {
			time.Sleep(time.Second)
		} else {
			log.Printf("Cannot contact with master for 10 times, stop.")
			os.Exit(0)
		}
		
	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":8080")
	
	sockname := masterSock()
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
