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

func (sck *ShardCtrler) bumpVersion() int64 {
	return atomic.AddInt64(&sck.version, 1)
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
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	v := cfg.String()
	sck.IKVClerk.Put("Config", v, rpc.Tversion(sck.version))
	sck.bumpVersion()
	raft.DPrintf("[SHARDCTRLR] Update Config: %s\n", v)
}

func (sck *ShardCtrler) handleRpcErr(err rpc.Err) bool {
	if err == rpc.OK {
		return true
	}
	if err == rpc.ErrStaleNum {
		panic("Unexpected RPC ERROR: ErrStaleNum")
	}
	return false
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
	for _, move := range moves {
		srcServer, srcExists := old.Groups[move.Src]
		dstServer, dstExists := new.Groups[move.Dst]
		if !srcExists || !dstExists {
			continue
		}

		srcClerk := shardgrp.MakeClerk(sck.clnt, srcServer)
		dstClerk := shardgrp.MakeClerk(sck.clnt, dstServer)

		var state []byte
		var err rpc.Err
		for {
			state, err = srcClerk.FreezeShard(move.Shard, new.Num)
			if sck.handleRpcErr(err) {
				break
			}
		}
		for {
			err = dstClerk.InstallShard(move.Shard, state, new.Num)
			if sck.handleRpcErr(err) {
				break
			}
		}
		for {
			err = srcClerk.DeleteShard(move.Shard, new.Num)
			if sck.handleRpcErr(err) {
				break
			}
		}
	}
	sck.InitConfig(new)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, _, err := sck.IKVClerk.Get("Config")
	if err != rpc.OK {
		return nil
	}
	config := shardcfg.FromString(value)
	return config
}
