package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	tester "6.5840/tester1"
)

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type CacheEntry struct {
	LastSeq   int64
	LastReply rpc.PutReply
}

type KVServer struct {
	me     int
	dead   int32
	rsm    *rsm.RSM
	mu     sync.RWMutex
	stores map[string]Entry
	cache  map[int64]CacheEntry
}

func (kv *KVServer) updateEntry(args *rpc.PutArgs) {
	kv.stores[args.Key] = Entry{
		args.Value,
		args.Version + 1,
	}
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case *rpc.GetArgs:
		return kv.doGet(req)
	case *rpc.PutArgs:
		return kv.doPut(req)
	}
	return nil
}

func (kv *KVServer) doGet(args *rpc.GetArgs) (reply rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	raft.DPrintf("[SERVER][GET ENTER] KVServer %d doGet key=%s", kv.me, args.Key)
	if entry, ok := kv.stores[args.Key]; ok {
		reply.Value, reply.Version = entry.Value, entry.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	raft.DPrintf("[SERVER][GET RETURN] KVServer %d doGet return %v", kv.me, reply)
	return
}

func (kv *KVServer) doPut(args *rpc.PutArgs) (reply rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	raft.DPrintf("[SERVER][PUT ENTER] KVServer %d doPut key=%s value=%s version=%d", kv.me, args.Key, args.Value, args.Version)
	if entry, ok := kv.cache[args.ClientID]; ok && args.Seq == entry.LastSeq {
		return entry.LastReply
	}
	if entry, ok := kv.stores[args.Key]; ok {
		if entry.Version == args.Version {
			kv.updateEntry(args)
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.updateEntry(args)
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
	kv.cache[args.ClientID] = CacheEntry{
		LastSeq:   args.Seq,
		LastReply: reply,
	}
	raft.DPrintf("[SERVER][PUT RETURN] KVServer %d doPut return err=%v", kv.me, reply.Err)
	return
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stores)
	e.Encode(kv.cache)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var stores map[string]Entry
	var cache map[int64]CacheEntry
	if d.Decode(&stores) != nil || d.Decode(&cache) != nil {
		panic("Failed to decode KVServer state from snapshot data")
	}
	kv.stores = stores
	kv.cache = cache
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(rpc.GetReply)
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(rpc.PutReply)
	} else {
		reply.Err = err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(&rpc.PutArgs{})
	labgob.Register(&rpc.GetArgs{})
	labgob.Register(Entry{})
	labgob.Register(map[string]Entry{})
	labgob.Register(CacheEntry{})
	labgob.Register(map[int64]CacheEntry{})

	kv := &KVServer{
		me:     me,
		stores: make(map[string]Entry),
		cache:  make(map[int64]CacheEntry),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
