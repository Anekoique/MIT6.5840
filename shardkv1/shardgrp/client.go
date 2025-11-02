package shardgrp

import (
	"math/rand"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const RetryInterval = 50 * time.Millisecond

type Clerk struct {
	clnt     *tester.Clnt
	servers  []string
	leader   int
	clientID int64
	seq      int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = 0
	ck.clientID = rand.Int63()
	ck.seq = 0
	return ck
}

func (ck *Clerk) bumpLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func (ck *Clerk) sendGet(args rpc.GetArgs) rpc.GetReply {
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.bumpLeader()
			time.Sleep(RetryInterval)
			continue
		}
		return reply
	}
}

func (ck *Clerk) sendPut(args rpc.PutArgs) rpc.Err {
	retried := false
	for {
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.Put", &args, &reply)
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

func (ck *Clerk) sendFreeze(args shardrpc.FreezeShardArgs) ([]byte, rpc.Err) {
	for {
		reply := shardrpc.FreezeShardReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.FreezeShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.bumpLeader()
			time.Sleep(RetryInterval)
			continue
		}
		return reply.State, reply.Err
	}
}

func (ck *Clerk) sendInstall(args shardrpc.InstallShardArgs) rpc.Err {
	for {
		reply := shardrpc.InstallShardReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.InstallShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.bumpLeader()
			time.Sleep(RetryInterval)
			continue
		}
		return reply.Err
	}
}

func (ck *Clerk) sendDelete(args shardrpc.DeleteShardArgs) rpc.Err {
	for {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.DeleteShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.bumpLeader()
			time.Sleep(RetryInterval)
			continue
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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	raft.DPrintf("[Client][FREEZE ENTER] Clerk FreezeShard shard=%d num=%d", s, num)
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	state, err := ck.sendFreeze(args)
	raft.DPrintf("[Client][FREEZE RETURN] Clerk FreezeShard shard=%d num=%d return err=%v", s, num, err)
	return state, err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	raft.DPrintf("[Client][INSTALL ENTER] Clerk InstallShard shard=%d num=%d", s, num)
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	err := ck.sendInstall(args)
	raft.DPrintf("[Client][INSTALL RETURN] Clerk InstallShard shard=%d num=%d return err=%v", s, num, err)
	return err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	raft.DPrintf("[Client][DELETE ENTER] Clerk DeleteShard shard=%d num=%d", s, num)
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	err := ck.sendDelete(args)
	raft.DPrintf("[Client][DELETE RETURN] Clerk DeleteShard shard=%d num=%d return err=%v", s, num, err)
	return err
}
