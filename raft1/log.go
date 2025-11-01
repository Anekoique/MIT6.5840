package raft

// log entry structure and log handling methods.

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Index   int
	Term    int
	Command any
}

// lastLog returns the last log entry.
func (rf *Raft) lastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

// firstLog returns the first log entry.
func (rf *Raft) firstLog() LogEntry {
	return rf.log[0]
}

// rebase converts a global log index to a local log slice index.
func (rf *Raft) rebase(index int) (int, bool) {
	base := rf.firstLog().Index
	end := rf.lastLog().Index
	if index < base || index > end {
		return -1, false
	}
	return index - base, true
}

// getEntry retrieves the log entry at the given global index.
func (rf *Raft) getEntry(index int) (LogEntry, bool) {
	rebased, ok := rf.rebase(index)
	if !ok {
		return LogEntry{}, false
	}
	return rf.log[rebased], true
}

// commitEntries attempts to advance the commitIndex based on matchIndex of followers.
func (rf *Raft) commitEntries() {
	for idx := rf.lastLog().Index; idx > rf.commitIndex; idx-- {
		entry, ok := rf.getEntry(idx)
		if !ok {
			continue
		}
		if entry.Term != rf.currentTerm {
			return
		}

		cnt := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= idx {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.logf("Committed entries up to index %d.", idx)
			rf.commitIndex = idx
			rf.applyCond.Broadcast()
			break
		}
	}
}
