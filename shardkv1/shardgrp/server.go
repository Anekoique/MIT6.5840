package shardgrp

import (
	"bytes"
	"maps"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type LastOp struct {
	Seq   int64
	Reply rpc.PutReply
}

type ShardState struct {
	Owned      bool
	Frozen     bool
	MaxNumSeen shardcfg.Tnum
	Stores     map[string]Entry
	LastOps    map[int64]LastOp
}

type Shard struct {
	mu sync.RWMutex
	ShardState
}

type ShardGroup struct {
	me     int
	dead   int32
	rsm    *rsm.RSM
	gid    tester.Tgid
	shards [shardcfg.NShards]*Shard
	// this field is set for debug
	id int64
}

func (grp *ShardGroup) getShard(key string) *Shard {
	sh := shardcfg.Key2Shard(key)
	return grp.shards[sh]
}

func (s *Shard) updateEntry(args *rpc.PutArgs) {
	s.Stores[args.Key] = Entry{
		args.Value,
		args.Version + 1,
	}
}

func (s *Shard) updateNum(num shardcfg.Tnum) bool {
	if num >= s.MaxNumSeen {
		s.MaxNumSeen = num
		return true
	}
	return false
}

func (s *Shard) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(s.Stores) != nil || e.Encode(s.LastOps) != nil {
		panic("Failed to encode Shard state to snapshot data")
	}
	return w.Bytes()
}

func (s *Shard) deserialize(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var stores map[string]Entry
	var lastOps map[int64]LastOp
	if d.Decode(&stores) != nil || d.Decode(&lastOps) != nil {
		panic("Failed to decode Shard state from snapshot data")
	}
	s.Stores = maps.Clone(stores)
	s.LastOps = maps.Clone(lastOps)
}

func (grp *ShardGroup) DoOp(req any) any {
	switch req := req.(type) {
	case *rpc.GetArgs:
		return grp.getShard(req.Key).doGet(req)
	case *rpc.PutArgs:
		return grp.getShard(req.Key).doPut(req)
	case *shardrpc.FreezeShardArgs:
		return grp.shards[req.Shard].doFreeze(req)
	case *shardrpc.InstallShardArgs:
		return grp.shards[req.Shard].doInstall(req)
	case *shardrpc.DeleteShardArgs:
		return grp.shards[req.Shard].doDelete(req)
	}
	return nil
}

func (s *Shard) doGet(args *rpc.GetArgs) (reply rpc.GetReply) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.Owned {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if entry, ok := s.Stores[args.Key]; ok {
		reply.Value, reply.Version = entry.Value, entry.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return
}

func (s *Shard) doPut(args *rpc.PutArgs) (reply rpc.PutReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.Owned {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if s.Frozen {
		reply.Err = rpc.ErrShardFrozen
		return
	}
	if op, ok := s.LastOps[args.ClientID]; ok && args.Seq == op.Seq {
		raft.DPrintf("[SHARDGROUP] Duplicate Put request detected: %v", op.Reply)
		return op.Reply
	}
	if entry, ok := s.Stores[args.Key]; ok {
		if entry.Version == args.Version {
			s.updateEntry(args)
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			s.updateEntry(args)
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
	s.LastOps[args.ClientID] = LastOp{
		Seq:   args.Seq,
		Reply: reply,
	}
	return
}

func (s *Shard) doFreeze(args *shardrpc.FreezeShardArgs) (reply shardrpc.FreezeShardReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.Owned {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if !s.updateNum(args.Num) {
		reply.Num, reply.Err = s.MaxNumSeen, rpc.ErrStaleNum
		return
	}
	s.Frozen = true
	reply.Num, reply.Err = s.MaxNumSeen, rpc.OK
	reply.State = s.serialize()
	return
}

func (s *Shard) doInstall(args *shardrpc.InstallShardArgs) (reply shardrpc.InstallShardReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.updateNum(args.Num) {
		reply.Err = rpc.ErrStaleNum
		return
	}
	s.deserialize(args.State)
	s.Owned, s.Frozen = true, false
	reply.Err = rpc.OK
	return
}

func (s *Shard) doDelete(args *shardrpc.DeleteShardArgs) (reply shardrpc.DeleteShardReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.updateNum(args.Num) {
		reply.Err = rpc.ErrStaleNum
		return
	}
	if !s.Owned {
		s.Frozen = false
		s.LastOps = make(map[int64]LastOp)
		reply.Err = rpc.OK
		return
	}
	s.Owned, s.Frozen = false, false
	s.Stores = make(map[string]Entry)
	s.LastOps = make(map[int64]LastOp)
	reply.Err = rpc.OK
	return
}

func (grp *ShardGroup) Snapshot() []byte {
	states := make([]ShardState, shardcfg.NShards)
	for i := 0; i < shardcfg.NShards; i++ {
		s := grp.shards[i]
		s.mu.RLock()
		state := s.ShardState
		state.Stores = maps.Clone(state.Stores)
		state.LastOps = maps.Clone(state.LastOps)
		states[i] = state
		s.mu.RUnlock()
	}

	w := new(bytes.Buffer)
	labgob.NewEncoder(w).Encode(states)
	return w.Bytes()
}

func (grp *ShardGroup) Restore(data []byte) {
	d := labgob.NewDecoder(bytes.NewReader(data))

	var states []ShardState
	if d.Decode(&states) != nil {
		panic("Failed to decode ShardGroup snapshot")
	}

	for i := 0; i < shardcfg.NShards; i++ {
		s := grp.shards[i]
		s.mu.Lock()
		s.ShardState = states[i]
		s.mu.Unlock()
	}
}

func handleRPC[A any, R any](grp *ShardGroup, op string, args *A, reply *R) {
	raft.DPrintf("[SERVER][%s ENTER] ShardGroup %d server=%d %s with args=%v", op, grp.id, grp.me, op, args)
	defer raft.DPrintf("[SERVER][%s RETURN] ShardGroup %d server=%d %s return with reply=%v", op, grp.id, grp.me, op, reply)

	err, res := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(R)
	} else {
		reflect.ValueOf(reply).Elem().FieldByName("Err").Set(reflect.ValueOf(err))
	}
}

func (grp *ShardGroup) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	handleRPC(grp, "GET", args, reply)
}

func (grp *ShardGroup) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	handleRPC(grp, "PUT", args, reply)
}

func (grp *ShardGroup) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	handleRPC(grp, "FREEZE", args, reply)
}

func (grp *ShardGroup) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	handleRPC(grp, "INSTALL", args, reply)
}

func (grp *ShardGroup) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	handleRPC(grp, "DELETE", args, reply)
}

func (grp *ShardGroup) Kill() {
	atomic.StoreInt32(&grp.dead, 1)
}

func (grp *ShardGroup) killed() bool {
	z := atomic.LoadInt32(&grp.dead)
	return z == 1
}

func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	labgob.Register(&rpc.PutArgs{})
	labgob.Register(&rpc.GetArgs{})
	labgob.Register(&shardrpc.FreezeShardArgs{})
	labgob.Register(&shardrpc.InstallShardArgs{})
	labgob.Register(&shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(Entry{})
	labgob.Register(ShardState{})
	labgob.Register(map[string]Entry{})
	labgob.Register(LastOp{})
	labgob.Register(map[int64]LastOp{})

	g := &ShardGroup{gid: gid, me: me, id: rand.Int63()}
	for i := 0; i < shardcfg.NShards; i++ {
		g.shards[i] = &Shard{
			ShardState: ShardState{
				Owned:      gid == shardcfg.Gid1,
				Frozen:     false,
				MaxNumSeen: 0,
				Stores:     make(map[string]Entry),
				LastOps:    make(map[int64]LastOp),
			},
		}
	}

	g.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, g)
	return []tester.IService{g, g.rsm.Raft()}
}
