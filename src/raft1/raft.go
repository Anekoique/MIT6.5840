// Package raft implements the Raft consensus algorithm.
package raft

// raft.go contains the core Raft peer structure and methods.

import (
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// RaftState represents the role of a Raft peer.
type (
	RaftState int
)

// RaftState contains the Follower, Candidate, and Leader states.
const (
	Follower RaftState = iota
	Candidate
	Leader
)

// Timing constants.
const (
	HeartbeatInterval = 100 * time.Millisecond
	ElectionTimeout   = 300 * time.Millisecond
	ElectionJitter    = 600 * time.Millisecond
)

// Raft is A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex            // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd   // RPC end points of all peers
	persister *tester.Persister     // Object to hold this peer's persisted state
	me        int                   // this peer's index into peers[]
	dead      int32                 // set by Kill()
	applyCh   chan raftapi.ApplyMsg // channel to send ApplyMsg messages to the service
	applyCond *sync.Cond            // Condition variable to signal the applier goroutine

	// Persistent state
	state       RaftState
	currentTerm int
	VotedFor    int
	log         []LogEntry

	//	Volatile state for all servers
	commitIndex int
	lastApplied int

	// Volatile state for leader
	nextIndex  []int
	matchIndex []int

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// toLeader transitions the server to the Leader state.
func (rf *Raft) toLeader() {
	rf.logf("Transitioned to Leader.")
	rf.state = Leader
	idx := rf.lastLog().Index
	for i := range rf.peers {
		rf.nextIndex[i] = idx + 1
		rf.matchIndex[i] = 0
	}
	rf.resetTimer(rf.heartbeatTimer)
	rf.electionTimer.Stop()
	rf.appendBroadcast()
}

// toCandidate transitions the server to the Candidate state.
func (rf *Raft) toCandidate() {
	rf.logf("Transitioned to Candidate.")
	rf.state = Candidate
	rf.currentTerm += 1
	rf.VotedFor = rf.me
	rf.persist(nil)
}

// toFollower transitions the server to the Follower state.
func (rf *Raft) toFollower() {
	if rf.state != Follower {
		rf.logf("Transitioned to Follower.")
		rf.state = Follower
		rf.heartbeatTimer.Stop()
	}
	rf.resetTimer(rf.electionTimer)
}

// updateTerm updates the current term if newTerm is greater than the current term.
func (rf *Raft) updateTerm(newTerm int) bool {
	if newTerm > rf.currentTerm {
		rf.logf("Discovered a newer term %d (our term is %d).", newTerm, rf.currentTerm)
		rf.currentTerm, rf.VotedFor = newTerm, -1
		rf.toFollower()
		rf.persist(nil)
		return true
	}
	return false
}

// isStateBehind checks if the server's state is behind based on term and state.
func (rf *Raft) isStateBehind(newTerm int, oldState RaftState, oldTerm int) bool {
	return rf.updateTerm(newTerm) || rf.state != oldState || rf.currentTerm != oldTerm
}

// Start initiates the agreement on the next command to be appended to the log.
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	term := rf.currentTerm
	index := rf.lastLog().Index + 1
	entry := LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	}
	rf.log = append(rf.log, entry)
	rf.logf("Leader Appended new entry at index %d: %v", index, entry)
	rf.persist(nil)
	rf.appendBroadcast()
	return index, term, true
}

// applier is a long-running goroutine that applies committed log entries to the state machine.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			break
		}

		commitIndex := rf.commitIndex
		commit, _ := rf.rebase(commitIndex)
		applied, _ := rf.rebase(rf.lastApplied)
		entries := slices.Clone(rf.log[applied+1 : commit+1])
		rf.logf("Applying from index %d to %d, entries: %v", applied, commit, entries)
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// electionTimeout returns a randomized election timeout duration.
func (rf *Raft) electionTimeout() time.Duration {
	timeout := ElectionTimeout + time.Duration(rand.Int63n(int64(ElectionJitter)))
	return timeout
}

// heartbeatTimeout returns the heartbeat timeout duration.
func (rf *Raft) heartbeatTimeout() time.Duration {
	return HeartbeatInterval
}

// resetTimer resets a timer safely.
func (rf *Raft) resetTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	switch t {
	case rf.electionTimer:
		t.Reset(rf.electionTimeout())
	case rf.heartbeatTimer:
		t.Reset(rf.heartbeatTimeout())
	default:
		panic("unknown timer")
	}
}

// appendBroadcast dispatches sendInstallSnapshot or appendOnce to all peers.
func (rf *Raft) appendBroadcast() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if rf.nextIndex[peer] <= rf.firstLog().Index {
			rf.logf("Heartbeat timer expired, sending installsnapshot to peer %d.", peer)
			go rf.sendInstallSnapshot(peer)
		} else {
			rf.logf("Heartbeat timer expired, sending heartbeats to peer %d.", peer)
			go rf.appendOnce(peer)
		}
	}
}

// ticker is a long-running goroutine that checks for election timeouts
// and sends heartbeats if this server is the leader.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.startElection()
				rf.resetTimer(rf.electionTimer)
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.appendBroadcast()
				rf.resetTimer(rf.heartbeatTimer)
			}
			rf.mu.Unlock()
		}
	}
}

// Kill stops this Raft peer.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	close(rf.applyCh)
	rf.mu.Unlock()
}

// killed checks if this Raft peer has been killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make creates a new Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	// init Raft struct
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		currentTerm: 0,
		VotedFor:    -1,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	// dummy log entry at index 0
	rf.log = append(rf.log, LogEntry{0, 0, nil})
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// initialize timers
	rf.electionTimer = time.NewTimer(rf.electionTimeout())
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatTimeout())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to apply committed entries
	go rf.applier()

	return rf
}
