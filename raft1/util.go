package raft

// conditional debug printing.

import (
	"fmt"
	"log"
	"time"
)

// Debugging flag.
const Debug = true

// DPrintf is a conditional debug print function.
func DPrintf(format string, a ...any) {
	if Debug {
		timestamp := time.Now().Format("15:04:05.000")
		args := append([]any{timestamp}, a...)
		log.Printf("%s "+format, args...)
	}
}

// String makes RaftState printable for debugging.
func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// logf formats a log message with a standard prefix.
func (rf *Raft) logf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[%d][T%d][%s] ", rf.me, rf.currentTerm, rf.state.String())
	format = prefix + format
	DPrintf(format, a...)
}
