// Package mr implements a MapReduce framework for a distributed systems lab.
// It includes a coordinator to manage tasks and workers to execute them.
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue is the basic unit and core concept of data processing
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is the entry of a worker unit to request a task
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		if response, ok := CallDispatch(WorkerArgs{nil, nil}); ok {
			switch response.phase {
			case MapPhase:
				doMapTask(mapf, response)
			case ReducePhase:
				doReduceTask(reducef, response)
			case WaitMap, WaitReduce:
				time.Sleep(1 * time.Second)
			case Done:
				return
			default:
				panic(fmt.Sprintf("Unexpected JobPhase %v\n", response.phase))
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
}

// A map worker unit
func doMapTask(mapf func(string, string) []KeyValue, reply *WorkerReply) {
	file, err := os.Open(reply.fileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.fileName)
	}
	file.Close()

	kva := mapf(reply.fileName, string(content))
	buckets := make([][]KeyValue, reply.nReduce)
	for _, kv := range kva {
		i := ihash(kv.Key) % reply.nReduce
		buckets[i] = append(buckets[i], kv)
	}

	for i, kva := range buckets {
		tmpfile, err := os.CreateTemp("./", "mr-map.tmp")
		if err != nil {
			log.Fatalf("cannot create tempfile")
		}
		encoder := json.NewEncoder(tmpfile)
		for _, kv := range kva {
			err := encoder.Encode(kv)
			if err != nil {
				log.Fatalf("unable to encode %v", kv)
			}
		}
		tmpfile.Close()
		fileName := fmt.Sprintf("mr-%d-%d", reply.ID, i)
		if err := os.Rename(tmpfile.Name(), fileName); err != nil {
			log.Fatalf("unable to rename tempfile %v", fileName)
		}
	}

	if _, ok := CallDispatch(WorkerArgs{&reply.ID, nil}); !ok {
		panic("Unexpected error")
	}
}

// A reduce worker unit
func doReduceTask(reducef func(string, []string) string, reply *WorkerReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.nMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reply.ID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	tmpfile, err := os.CreateTemp("./", "mr-reduce.tmp")
	if err != nil {
		log.Fatalf("cannot create tempfile")
	}

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

		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)
	}
	tmpfile.Close()
	fileName := fmt.Sprintf("mr-out-%d", reply.ID)
	if err := os.Rename(tmpfile.Name(), fileName); err != nil {
		log.Fatalf("unable to rename tempfile %v", fileName)
	}

	if _, ok := CallDispatch(WorkerArgs{nil, &reply.ID}); !ok {
		panic("Unexpected error")
	}
}

func CallDispatch(args WorkerArgs) (reply *WorkerReply, ok bool) {
	ok = call("Coordinator.Dispatch", &args, &reply)
	return
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
