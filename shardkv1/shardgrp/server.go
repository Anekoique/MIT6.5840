package shardgrp

import (
	"bytes"
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

type CacheEntry struct {
	LastSeq   int64
	LastReply rpc.PutReply
}

type Shard struct {
	mu         sync.RWMutex
	owned      bool
	frozen     bool
	maxNumSeen shardcfg.Tnum

	stores map[string]Entry
	cache  map[int64]CacheEntry
}

type ShardGroup struct {
	me     int
	dead   int32
	rsm    *rsm.RSM
	gid    tester.Tgid
	shards [shardcfg.NShards]*Shard
}

func (grp *ShardGroup) getShard(key string) *Shard {
	sh := shardcfg.Key2Shard(key)
	return grp.shards[sh]
}

func (s *Shard) updateEntry(args *rpc.PutArgs) {
	s.stores[args.Key] = Entry{
		args.Value,
		args.Version + 1,
	}
}

func (s *Shard) updateNum(num shardcfg.Tnum) bool {
	if num >= s.maxNumSeen {
		s.maxNumSeen = num
		return true
	}
	return false
}

func (s *Shard) pack() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(s.stores)
	e.Encode(s.cache)
	return w.Bytes()
}

func (s *Shard) unpack(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var stores map[string]Entry
	var cache map[int64]CacheEntry
	if d.Decode(&stores) != nil || d.Decode(&cache) != nil {
		panic("Failed to decode Shard state from snapshot data")
	}
	s.stores = stores
	s.cache = cache
}

func (grp *ShardGroup) DoOp(req any) any {
	switch req := req.(type) {
	case *rpc.GetArgs:
		s := grp.getShard(req.Key)
		return s.doGet(req)
	case *rpc.PutArgs:
		s := grp.getShard(req.Key)
		return s.doPut(req)
	case *shardrpc.FreezeShardArgs:
		s := grp.shards[req.Shard]
		return s.doFreeze(req)
	case *shardrpc.InstallShardArgs:
		s := grp.shards[req.Shard]
		return s.doInstall(req)
	case *shardrpc.DeleteShardArgs:
		s := grp.shards[req.Shard]
		return s.doDelete(req)
	}
	return nil
}

func (s *Shard) doGet(args *rpc.GetArgs) (reply rpc.GetReply) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if entry, ok := s.stores[args.Key]; ok {
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
	if !s.owned {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if s.frozen {
		reply.Err = rpc.ErrShardFrozen
		return
	}
	if entry, ok := s.cache[args.ClientID]; ok && args.Seq == entry.LastSeq {
		return entry.LastReply
	}
	if entry, ok := s.stores[args.Key]; ok {
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
	s.cache[args.ClientID] = CacheEntry{
		LastSeq:   args.Seq,
		LastReply: reply,
	}
	return
}

func (s *Shard) doFreeze(args *shardrpc.FreezeShardArgs) (reply shardrpc.FreezeShardReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.owned {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if !s.updateNum(args.Num) {
		reply.Num, reply.Err = s.maxNumSeen, rpc.ErrStaleNum
		return
	}
	s.frozen = true
	reply.Num, reply.Err = s.maxNumSeen, rpc.OK
	reply.State = s.pack()
	return
}

func (s *Shard) doInstall(args *shardrpc.InstallShardArgs) (reply shardrpc.InstallShardReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.updateNum(args.Num) {
		reply.Err = rpc.ErrStaleNum
		return
	}
	s.unpack(args.State)
	s.owned, s.frozen = true, false
	reply.Err = rpc.OK
	return
}

func (s *Shard) doDelete(args *shardrpc.DeleteShardArgs) (reply shardrpc.DeleteShardReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.owned {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if !s.updateNum(args.Num) {
		reply.Err = rpc.ErrStaleNum
		return
	}
	s.owned, s.frozen = false, false
	s.stores = make(map[string]Entry)
	s.cache = make(map[int64]CacheEntry)
	reply.Err = rpc.OK
	return
}

type shardHdr struct {
	Owned  bool
	Frozen bool
	Num    shardcfg.Tnum
	Blob   []byte
}

func (grp *ShardGroup) Snapshot() []byte {
	hdrs := make([]shardHdr, shardcfg.NShards)
	for i := 0; i < shardcfg.NShards; i++ {
		s := grp.shards[i]
		s.mu.RLock()
		hdrs[i] = shardHdr{
			Owned:  s.owned,
			Frozen: s.frozen,
			Num:    s.maxNumSeen,
			Blob:   s.pack(),
		}
		s.mu.RUnlock()
	}
	var w bytes.Buffer
	e := labgob.NewEncoder(&w)
	e.Encode(hdrs)
	return w.Bytes()
}

func (grp *ShardGroup) Restore(data []byte) {
	var hdrs []shardHdr
	d := labgob.NewDecoder(bytes.NewReader(data))
	if d.Decode(&hdrs) != nil || len(hdrs) != shardcfg.NShards {
		panic("Failed to decode ShardGroup snapshot")
	}
	for i := 0; i < shardcfg.NShards; i++ {
		s := grp.shards[i]
		h := hdrs[i]
		s.mu.Lock()
		s.owned, s.frozen, s.maxNumSeen = h.Owned, h.Frozen, h.Num
		s.unpack(h.Blob)
		s.mu.Unlock()
	}
}

func (grp *ShardGroup) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	raft.DPrintf("[SERVER][GET ENTER] ShardGroup %d Get key=%s", grp.me, args.Key)
	defer raft.DPrintf("[SERVER][GET RETURN] ShardGroup %d Get return %v", grp.me, reply)

	err, res := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(rpc.GetReply)
	} else {
		reply.Err = err
	}
}

func (grp *ShardGroup) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	raft.DPrintf("[SERVER][PUT ENTER] ShardGroup %d Put key=%s", grp.me, args.Key)
	defer raft.DPrintf("[SERVER][PUT RETURN] ShardGroup %d Put return %v", grp.me, reply)

	err, res := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(rpc.PutReply)
	} else {
		reply.Err = err
	}
}

func (grp *ShardGroup) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	raft.DPrintf("[SERVER][FREEZE ENTER] ShardGroup %d FreezeShard shard=%d", grp.me, args.Shard)
	err, res := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(shardrpc.FreezeShardReply)
	} else {
		reply.Err = err
	}
	raft.DPrintf("[SERVER][FREEZE RETURN] ShardGroup %d FreezeShard shard=%d return err=%v", grp.me, args.Shard, reply.Err)
}

func (grp *ShardGroup) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	raft.DPrintf("[SERVER][INSTALL ENTER] ShardGroup %d InstallShard shard=%d", grp.me, args.Shard)
	defer raft.DPrintf("[SERVER][INSTALL RETURN] ShardGroup %d InstallShard shard=%d return %v", grp.me, args.Shard, reply)

	err, res := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(shardrpc.InstallShardReply)
	} else {
		reply.Err = err
	}
}

func (grp *ShardGroup) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	raft.DPrintf("[SERVER][DELETE ENTER] ShardGroup %d DeleteShard shard=%d", grp.me, args.Shard)
	defer raft.DPrintf("[SERVER][DELETE RETURN] ShardGroup %d DeleteShard shard=%d return %v", grp.me, args.Shard, reply)

	err, res := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = res.(shardrpc.DeleteShardReply)
	} else {
		reply.Err = err
	}
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
	labgob.Register(CacheEntry{})
	labgob.Register(shardHdr{})
	labgob.Register(map[string]Entry{})
	labgob.Register(map[int64]CacheEntry{})

	g := &ShardGroup{gid: gid, me: me}
	for i := 0; i < shardcfg.NShards; i++ {
		g.shards[i] = &Shard{
			stores: make(map[string]Entry),
			cache:  make(map[int64]CacheEntry),
		}
		if gid == shardcfg.Gid1 {
			g.shards[i].owned = true
		}
	}

	g.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, g)
	return []tester.IService{g, g.rsm.Raft()}
}
