package kvraft

import (
	"math/rand"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	raft "6.5840/raft1"
	tester "6.5840/tester1"
)

const RetryInterval = 50 * time.Millisecond

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int

	clientID int64
	seq      int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = 0
	ck.clientID = rand.Int63()
	ck.seq = 0
	return ck
}

func (ck *Clerk) bumpLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) sendGet(args rpc.GetArgs) rpc.GetReply {
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.bumpLeader()
			time.Sleep(RetryInterval)
			continue
		}
		return reply
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) sendPut(args rpc.PutArgs) rpc.Err {
	retried := false
	for {
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			retried = true
			ck.bumpLeader()
			time.Sleep(RetryInterval)
			continue
		}
		if reply.Err == rpc.ErrVersion && retried {
			return rpc.ErrMaybe
		}
		return reply.Err
	}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	raft.DPrintf("[Client][GET ENTER] Clerk Get key=%s", key)
	reply := ck.sendGet(rpc.GetArgs{Key: key})
	raft.DPrintf("[Client][GET RETURN] Clerk Get key=%s return %v", key, reply)
	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	raft.DPrintf("[Client][PUT ENTER] Clerk Put key=%s value=%s version=%d", key, value, version)
	seq := atomic.AddInt64(&ck.seq, 1)
	putArgs := rpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		ClientID: ck.clientID,
		Seq:      seq,
	}
	err := ck.sendPut(putArgs)
	raft.DPrintf("[Client][PUT RETURN] Clerk Put key=%s value=%s version=%d return %v", key, value, version, err)
	return err
}
