package raft

// RPC argument and reply structures.

// RequestVoteArgs: the argument structure for RequestVote RPCs.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply: the reply structure for RequestVote RPCs.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs: the argument structure for AppendEntries RPCs.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply: the reply structure for AppendEntries RPCs.
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// InstallSnapshotArgs: the argument structure for InstallSnapshot RPCs.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply: the reply structure for InstallSnapshot RPCs.
type InstallSnapshotReply struct {
	Term int
}
