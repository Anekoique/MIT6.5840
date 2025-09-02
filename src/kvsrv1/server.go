// Package kvsrv impl kvserver
package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]Entry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = make(map[string]Entry)
	return kv
}

func (kv *KVServer) updateEntry(args *rpc.PutArgs) {
	kv.data[args.Key] = Entry{
		args.Value,
		args.Version + 1,
	}
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if entry, ok := kv.data[args.Key]; ok {
		reply.Value, reply.Version = entry.Value, entry.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Put Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if entry, ok := kv.data[args.Key]; ok {
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
}

// Kill the server
// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// StartKVServer start a kvserver
// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
