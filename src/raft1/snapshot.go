package raft

// snapshot handling for Raft.

import (
	"slices"

	"6.5840/raftapi"
)

// Snapshot tells Raft that it has created a snapshot up to and including index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.firstLog().Index {
		return
	}

	rebased, ok := rf.rebase(index)
	if !ok {
		return
	}
	rf.log = slices.Clone(rf.log[rebased:])
	rf.persist(snapshot)
	rf.logf("Created snapshot up to index %d.", index)
}

// sendInstallSnapshot sends an InstallSnapshot RPC to a server.
func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.firstLog().Index,
		LastIncludedTerm:  rf.firstLog().Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isStateBehind(reply.Term, Leader, args.Term) {
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

// InstallSnapshot handles an InstallSnapshot RPC request.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.toFollower()
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	rf.logf("Installing snapshot (lastIncludedIndex: %d, lastIncludedTerm: %d).", args.LastIncludedIndex, args.LastIncludedTerm)
	if args.LastIncludedIndex > rf.lastLog().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		idx, _ := rf.rebase(args.LastIncludedIndex)
		rf.log = rf.log[idx:]
	}
	rf.log[0].Index, rf.log[0].Term = args.LastIncludedIndex, args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist(args.Data)

	go func() {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}
