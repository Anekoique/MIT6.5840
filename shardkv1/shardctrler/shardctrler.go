package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"
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

	mu         sync.Mutex
	recovering atomic.Bool
	version    int64
}

// BumpVersion increments the version after updating the configuration.
func (sck *ShardCtrler) BumpVersion(cfg *shardcfg.ShardConfig) {
	sck.UpdateConfig(cfg, "Config")
	atomic.AddInt64(&sck.version, 1)
}

// SetVersion sets the current version.
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

// Called once by the tester to initialize the controller.
func (sck *ShardCtrler) InitController() {
	currentCfg := sck.Query()
	_, nextCfg := sck.QueryNext()
	if nextCfg != nil && nextCfg.Num > currentCfg.Num {
		sck.mu.Lock()
		defer sck.mu.Unlock()
		sck.ChangeConfigTo(nextCfg)
	}
}

// Called by the tester to ask the controller to initialize
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	v := cfg.String()
	sck.IKVClerk.Put("Next", "", rpc.Tversion(sck.version))
	sck.BumpVersion(cfg)
	raft.DPrintf("[SHARDCTRLR] Init Config: %s\n", v)
}

// Called by the tester to ask the controller to update
func (sck *ShardCtrler) UpdateConfig(cfg *shardcfg.ShardConfig, key string) rpc.Err {
	v := cfg.String()
	err := sck.IKVClerk.Put(key, v, rpc.Tversion(sck.version))
	raft.DPrintf("[SHARDCTRLR] Update Config: %s\n", v)
	return err
}

// ChangeConfigTo changes the configuration to the new configuration.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	old := sck.Query()
	if !sck.claimNextOwnership(old, new) {
		return
	}
	moves := old.Diff(new)
	raft.DPrintf("[SHARDCTRLR] Moves: %v\n", moves)
	for _, move := range moves {
		if !sck.migrate(old, new, move) && sck.hasConfigApplied(new.Num) {
			return
		}
	}
	sck.BumpVersion(new)
}

// Query fetches the latest configuration from the shard controller
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, version, err := sck.IKVClerk.Get("Config")
	if err != rpc.OK {
		return nil
	}
	sck.SetVersion(version)
	config := shardcfg.FromString(value)
	return config
}

// QueryNext fetches the "Next" configuration from the shard controller
func (sck *ShardCtrler) QueryNext() (string, *shardcfg.ShardConfig) {
	value, version, err := sck.IKVClerk.Get("Next")
	if err != rpc.OK {
		return "", nil
	}
	sck.SetVersion(version)
	if value == "" {
		return "", nil
	}
	config := shardcfg.FromString(value)
	return value, config
}

// hasConfigApplied checks if the configuration with the given number has been applied.
func (sck *ShardCtrler) hasConfigApplied(num shardcfg.Tnum) bool {
	cfg := sck.Query()
	return cfg != nil && cfg.Num >= num
}

// claimNextOwnership tries to claim ownership of the "Next" configuration.
func (sck *ShardCtrler) claimNextOwnership(old, new *shardcfg.ShardConfig) bool {
	nextStr := new.String()

	for {
		nextValue, nextCfg := sck.QueryNext()
		if nextCfg != nil && nextCfg.Num > old.Num {
			return new.Num == nextCfg.Num && nextValue == nextStr
		}
		err := sck.UpdateConfig(new, "Next")
		if err == rpc.OK {
			return true
		}
		if err == rpc.ErrMaybe {
			if nextValue, nextCfg := sck.QueryNext(); nextCfg != nil && nextCfg.Num == new.Num && nextValue == nextStr {
				return true
			}
			continue
		}
		if err == rpc.ErrVersion {
			continue
		}
		return false
	}
}

// migrate migrates a shard from the source group to the destination group.
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
