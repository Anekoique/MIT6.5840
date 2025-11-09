package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	mu       sync.Mutex
	clnt     *tester.Clnt
	sck      *shardctrler.ShardCtrler
	clientID int64
	seq      int64

	config    shardcfg.ShardConfig
	grpClerks map[tester.Tgid]*grpClerk
}

type grpClerk struct {
	servers []string
	clerk   *shardgrp.Clerk
}

func (ck *Clerk) updateConfig() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	newConfig := ck.sck.Query()
	if newConfig == nil {
		return
	}
	if newConfig.Num > ck.config.Num {
		ck.config = *newConfig
		ck.pruneCache()
	}
}

func (ck *Clerk) pruneCache() {
	if len(ck.grpClerks) == 0 {
		return
	}
	for gid := range ck.grpClerks {
		if _, ok := ck.config.Groups[gid]; !ok {
			delete(ck.grpClerks, gid)
		}
	}
}

func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:      clnt,
		sck:       sck,
		clientID:  rand.Int63(),
		seq:       0,
		grpClerks: make(map[tester.Tgid]*grpClerk),
	}
	ck.config = *ck.sck.Query()
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		grpCk := ck.genGrpClerk(key)
		value, version, err := grpCk.Get(key)
		if err == rpc.ErrWrongGroup || err == rpc.ErrShardFrozen || err == rpc.ErrUnreachable {
			if err == rpc.ErrShardFrozen {
				time.Sleep(50 * time.Millisecond)
			}
			ck.updateConfig()
			continue
		}
		return value, version, err
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	seq := atomic.AddInt64(&ck.seq, 1)
	for {
		grpCk := ck.genGrpClerk(key)
		err := grpCk.Put(key, value, version, ck.clientID, seq)
		if err == rpc.ErrWrongGroup || err == rpc.ErrShardFrozen || err == rpc.ErrUnreachable {
			if err == rpc.ErrShardFrozen {
				time.Sleep(50 * time.Millisecond)
			}
			ck.updateConfig()
			continue
		}
		return err
	}
}

func (ck *Clerk) genGrpClerk(key string) *shardgrp.Clerk {
	shard := shardcfg.Key2Shard(key)

	ck.mu.Lock()
	defer ck.mu.Unlock()

	gid, srv, _ := ck.config.GidServers(shard)
	entry, ok := ck.grpClerks[gid]
	if !ok || !slices.Equal(entry.servers, srv) {
		srvCopy := append([]string(nil), srv...)
		entry = &grpClerk{
			servers: srvCopy,
			clerk:   shardgrp.MakeClerk(ck.clnt, srvCopy),
		}
		ck.grpClerks[gid] = entry
	}
	raft.DPrintf("[Client] route key=%s shard=%d gid=%d servers=%v", key, shard, gid, entry.servers)
	return entry.clerk
}
