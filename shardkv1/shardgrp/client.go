package shardgrp

import (
	"reflect"
	"time"

	"6.5840/kvsrv1/rpc"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	RetryInterval  = 10 * time.Millisecond
	requestTimeout = 1 * time.Second
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = 0
	return ck
}

func (ck *Clerk) bumpLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func sendRPC[A any, R any](ck *Clerk, method string, args *A, reply *R) rpc.Err {
	raft.DPrintf("[Client][%s ENTRY] Clerk %s with args=%v", method, method, args)
	defer raft.DPrintf("[Client][%s RETURN] Clerk %s return with reply=%v", method, method, reply)

	timeout := time.NewTimer(requestTimeout)
	retried := false
	var zero R
	for {
		select {
		case <-timeout.C:
			return rpc.ErrWrongGroup
		default:
			*reply = zero
			ok := ck.clnt.Call(ck.servers[ck.leader], method, args, reply)
			err := reflect.ValueOf(reply).Elem().FieldByName("Err").Interface().(rpc.Err)
			if !ok || err == rpc.ErrWrongLeader {
				ck.bumpLeader()
				retried = true
				time.Sleep(RetryInterval)
				continue
			}
			if err == rpc.ErrVersion && retried {
				return rpc.ErrMaybe
			}
			return err
		}
	}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	err := sendRPC(ck, "ShardGroup.Get", &args, &reply)
	return reply.Value, reply.Version, err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion, clientID int64, seq int64) rpc.Err {
	putArgs := rpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		ClientID: clientID,
		Seq:      seq,
	}
	reply := rpc.PutReply{}
	err := sendRPC(ck, "ShardGroup.Put", &putArgs, &reply)
	return err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	reply := shardrpc.FreezeShardReply{}
	err := sendRPC(ck, "ShardGroup.FreezeShard", &args, &reply)
	return reply.State, err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	reply := shardrpc.InstallShardReply{}
	err := sendRPC(ck, "ShardGroup.InstallShard", &args, &reply)
	return err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	reply := shardrpc.DeleteShardReply{}
	err := sendRPC(ck, "ShardGroup.DeleteShard", &args, &reply)
	return err
}
