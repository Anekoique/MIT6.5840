package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync/atomic"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed  int32 // set by Kill()
	version int64
}

func (sck *ShardCtrler) BumpVersion() int64 {
	return atomic.AddInt64(&sck.version, 1)
}

func (sck *ShardCtrler) SetVersion(v rpc.Tversion) {
	atomic.StoreInt64(&sck.version, int64(v))
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	currentCfg := sck.Query()
	nextCfg := sck.QueryNext()
	if nextCfg != nil && nextCfg.Num > currentCfg.Num {
		sck.ChangeConfigTo(nextCfg)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	v := cfg.String()
	sck.IKVClerk.Put("Config", v, rpc.Tversion(sck.version))
	sck.IKVClerk.Put("Next", "", rpc.Tversion(sck.version))
	sck.BumpVersion()
	raft.DPrintf("[SHARDCTRLR] Init Config: %s\n", v)
}

func (sck *ShardCtrler) UpdateConfig(cfg *shardcfg.ShardConfig, key string) {
	v := cfg.String()
	sck.IKVClerk.Put(key, v, rpc.Tversion(sck.version))
	raft.DPrintf("[SHARDCTRLR] Update Config: %s\n", v)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	old := sck.Query()
	if new.Num != old.Num+1 {
		panic("ChangeConfigTo: new.Num != old.Num + 1")
	}
	moves := old.Diff(new)
	raft.DPrintf("[SHARDCTRLR] Moves: %v\n", moves)
	sck.UpdateConfig(new, "Next")
	for _, move := range moves {
		if !sck.migrate(old, new, move) && sck.hasConfigApplied(new.Num) {
			return
		}
	}
	sck.UpdateConfig(new, "Config")
	sck.BumpVersion()
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, version, err := sck.IKVClerk.Get("Config")
	if err != rpc.OK {
		return nil
	}
	sck.SetVersion(version)
	config := shardcfg.FromString(value)
	return config
}

func (sck *ShardCtrler) QueryNext() *shardcfg.ShardConfig {
	value, version, err := sck.IKVClerk.Get("Next")
	if err != rpc.OK {
		return nil
	}
	sck.SetVersion(version)
	if value == "" {
		return nil
	}
	config := shardcfg.FromString(value)
	return config
}

func (sck *ShardCtrler) hasConfigApplied(num shardcfg.Tnum) bool {
	cfg := sck.Query()
	return cfg != nil && cfg.Num >= num
}

func (sck *ShardCtrler) migrate(old, new *shardcfg.ShardConfig, move shardcfg.Move) bool {
	srcServer, srcExists := old.Groups[move.Src]
	dstServer, dstExists := new.Groups[move.Dst]
	if !srcExists || !dstExists {
		return false
	}

	srcClerk := shardgrp.MakeClerk(sck.clnt, srcServer)
	dstClerk := shardgrp.MakeClerk(sck.clnt, dstServer)

	var state []byte
	var err rpc.Err
	for {
		if sck.hasConfigApplied(new.Num) {
			return false
		}
		state, err = srcClerk.FreezeShard(move.Shard, new.Num)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongGroup || err == rpc.ErrStaleNum {
			return false
		}
	}
	for {
		if sck.hasConfigApplied(new.Num) {
			return false
		}
		err = dstClerk.InstallShard(move.Shard, state, new.Num)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongGroup || err == rpc.ErrStaleNum {
			return false
		}
	}
	for {
		if sck.hasConfigApplied(new.Num) {
			return false
		}
		err = srcClerk.DeleteShard(move.Shard, new.Num)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongGroup || err == rpc.ErrStaleNum {
			return false
		}
	}
	return true
}
