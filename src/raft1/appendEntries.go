package raft

// appendEntries handling for Raft.

import "slices"

// AppendEntries handles an AppendEntries RPC request.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm, reply.XIndex, reply.XLen = -1, -1, rf.lastLog().Index+1
	if args.Term < rf.currentTerm {
		return
	}

	rf.toFollower()
	if args.PrevLogIndex > rf.lastLog().Index {
		reply.XLen = rf.lastLog().Index + 1
		return
	}

	idx, ok := rf.rebase(args.PrevLogIndex)
	if !ok {
		reply.XLen = rf.firstLog().Index + 1
		return
	}
	if rf.log[idx].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[idx].Term
		reply.XIndex = rf.findFirstIndexOfTerm(idx, reply.XTerm)
		return
	}

	reply.Success = true
	if len(args.Entries) != 0 && rf.isConflict(args) {
		rf.logf("Appended %d new entries from leader %d starting at index %d.", len(args.Entries), args.LeaderID, args.PrevLogIndex+1)
		rf.log = append(rf.log[:idx+1], args.Entries...)
		rf.persist(nil)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLog().Index)
		rf.logf("Commit index advanced to %d by leader %d.", rf.commitIndex, args.LeaderID)
		rf.applyCond.Broadcast()
	}
}

// appendOnce sends an AppendEntries RPC to a server once.
func (rf *Raft) appendOnce(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	index, ok := rf.rebase(rf.nextIndex[server] - 1)
	if !ok {
		rf.mu.Unlock()
		return
	}
	entries := slices.Clone(rf.log[index+1:])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.log[index].Index,
		PrevLogTerm:  rf.log[index].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isStateBehind(reply.Term, Leader, args.Term) {
		return
	}
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.commitEntries()
		return
	}

	if reply.XTerm == -1 {
		rf.nextIndex[server] = reply.XLen
	} else {
		lastIndex := rf.findLastIndexOfTerm(reply.XTerm)
		if lastIndex == -1 {
			rf.nextIndex[server] = reply.XIndex
		} else {
			rf.nextIndex[server] = lastIndex + 1
		}
	}
}

// findFirstIndexOfTerm finds the first index with the given term
func (rf *Raft) findFirstIndexOfTerm(idx, term int) int {
	for idx > 0 && rf.log[idx-1].Term == term {
		idx--
	}
	return rf.log[idx].Index
}

// findLastIndexOfTerm finds the last index with the given term.
func (rf *Raft) findLastIndexOfTerm(term int) int {
	for idx := len(rf.log) - 1; idx >= 0; idx-- {
		if rf.log[idx].Term == term {
			return rf.log[idx].Index
		}
	}
	return -1
}

// isConflict checks if there is a conflict between the log entries and the incoming AppendEntries RPC.
func (rf *Raft) isConflict(args *AppendEntriesArgs) bool {
	base := args.PrevLogIndex + 1
	for i, incoming := range args.Entries {
		local, ok := rf.getEntry(base + i)
		if !ok {
			return true
		}
		if local.Term != incoming.Term {
			return true
		}
	}
	return false
}
