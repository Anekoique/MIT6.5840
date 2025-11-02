package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"

	// For shardkv lab
	ErrShardFrozen = "ErrShardFrozen"
	ErrStaleNum    = "ErrStaleNum"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion

	ClientID int64
	Seq      int64
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
