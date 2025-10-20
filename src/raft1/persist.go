package raft

// persistent state handling for Raft.

import (
	"bytes"

	"6.5840/labgob"
)

// persist saves Raft's persistent state to stable storage,
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)

	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("readPersist failed\n")
	} else {
		rf.currentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.log = log
		rf.lastApplied = rf.firstLog().Index
		rf.commitIndex = rf.firstLog().Index
	}
}

// PersistBytes returns the size of Raft's persistent state in bytes.
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
