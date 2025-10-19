package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// RaftState represents the role of a Raft peer.
type (
	RaftState int
)

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

// RaftState constants.
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

// electionTimeout returns a randomized election timeout duration.
func (rf *Raft) electionTimeout() time.Duration {
	timeout := ElectionTimeout + time.Duration(rand.Int63n(int64(ElectionJitter)))
	return timeout
}

// heartbeatTimeout returns the heartbeat timeout duration.
func (rf *Raft) heartbeatTimeout() time.Duration {
	return HeartbeatInterval
}

// wrapper to reset a timer safely
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

// logf formats a log message with a standard prefix.
// The caller MUST hold rf.mu.
func (rf *Raft) logf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[%d][T%d][%s] ", rf.me, rf.currentTerm, rf.state.String())
	format = prefix + format
	DPrintf(format, a...)
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Index   int
	Command any
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// lastLog returns the last log entry.
func (rf *Raft) lastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

// toLeader transitions the server to the Leader state.
func (rf *Raft) toLeader() {
	rf.logf("Transitioned to Leader. current logs: %v", rf.log)
	rf.state = Leader
	index := len(rf.log)
	for i := range rf.peers {
		rf.nextIndex[i] = index
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
	rf.persist()
}

// toFollower transitions the server to the Follower state.
func (rf *Raft) toFollower() {
	rf.logf("Transitioned to Follower.")
	rf.state = Follower
	rf.resetTimer(rf.electionTimer)
	rf.heartbeatTimer.Stop()
}

// updateTerm updates the current term if newTerm is greater than the current term.
func (rf *Raft) updateTerm(newTerm int) bool {
	if newTerm > rf.currentTerm {
		rf.logf("Discovered a newer term %d (our term is %d). Transitioning to Follower.", newTerm, rf.currentTerm)
		rf.currentTerm, rf.VotedFor = newTerm, -1
		rf.toFollower()
		rf.persist()
		return true
	}
	return false
}

// persist saves Raft's persistent state to stable storage,
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term == rf.currentTerm && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) {
		lastLogIndex := rf.lastLog().Index
		lastLogTerm := rf.lastLog().Term

		granted := args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		rf.logf("Granted: %v vote to candidate %d. args: %v, lastLogIndex: %d, lastLogTerm: %d", granted, args.CandidateID, args, lastLogIndex, lastLogTerm)
		if granted {
			rf.VotedFor = args.CandidateID
			reply.VoteGranted = true
			rf.resetTimer(rf.electionTimer)
			rf.persist()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.log)
	if args.Term < rf.currentTerm {
		rf.logf("Rejected AppendEntries from %d: sender's term %d is stale (our term is %d).", args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	rf.state = Follower
	rf.resetTimer(rf.electionTimer)

	if args.PrevLogIndex >= len(rf.log) {
		rf.logf("Rejected AppendEntries from %d: PrevLogIndex %d is out of bounds (our log len is %d).", args.LeaderID, args.PrevLogIndex, len(rf.log))
		reply.XLen = len(rf.log)
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logf("Rejected AppendEntries from %d: Term mismatch at PrevLogIndex %d.", args.LeaderID, args.PrevLogIndex)
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		xIndex := args.PrevLogIndex
		for xIndex > 0 && rf.log[xIndex-1].Term == reply.XTerm {
			xIndex--
		}
		reply.XIndex = xIndex
		return
	}

	reply.Success = true
	if len(args.Entries) != 0 && rf.reconcileLogLocked(args) {
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.logf("Updated commitIndex to %d.", rf.commitIndex)
		rf.applyCond.Broadcast()
	}
}

// reconcileLogLocked integrates entries from the leader into the follower's log.
// The caller MUST hold rf.mu. It returns true if the log was modified.
func (rf *Raft) reconcileLogLocked(args *AppendEntriesArgs) bool {
	insertIdx := args.PrevLogIndex + 1
	offset := 0

	for ; offset < len(args.Entries); offset++ {
		localIdx := insertIdx + offset
		if localIdx >= len(rf.log) {
			break
		}
		if rf.log[localIdx].Term != args.Entries[offset].Term {
			rf.log = rf.log[:localIdx]
			break
		}
	}

	if offset < len(args.Entries) {
		newEntries := append([]LogEntry(nil), args.Entries[offset:]...)
		rf.log = append(rf.log, newEntries...)
		rf.logf("Appended %d entries from leader %d starting at index %d.", len(newEntries), args.LeaderID, insertIdx+offset)
		return true
	}

	return false
}

func (rf *Raft) appendOnce(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevIndex := rf.nextIndex[server] - 1
	prevTerm := rf.log[prevIndex].Term
	var entries []LogEntry
	if rf.nextIndex[server] < len(rf.log) && prevIndex+1 < len(rf.log) {
		entries = append([]LogEntry(nil), rf.log[prevIndex+1:]...)
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.logf("Sending AppendEntries to peer %d (entries: %d, prevLogIndex: %v).", server, entries, prevIndex)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if !rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
		return
	}

	rf.mu.Lock()
	if rf.updateTerm(reply.Term) || args.Term != rf.currentTerm || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		matchIdx := args.PrevLogIndex + len(args.Entries)
		if matchIdx > rf.matchIndex[server] {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.updateCommitIndexLocked()
		}
		rf.mu.Unlock()
		return
	}
	if reply.XTerm == -1 {
		rf.nextIndex[server] = max(1, reply.XLen)
	} else {
		lastIndex := rf.findLastIndexOfTerm(reply.XTerm)
		if lastIndex == -1 {
			rf.nextIndex[server] = reply.XIndex
		} else {
			rf.nextIndex[server] = lastIndex + 1
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) appendBroadcast() {
	rf.logf("Heartbeat timer expired, sending heartbeats.")
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.appendOnce(i)
	}
}

// findLastIndexOfTerm returns the highest index in rf.log whose entry has the given term.
// It returns -1 if the term does not exist in the log.
func (rf *Raft) findLastIndexOfTerm(term int) int {
	for idx := len(rf.log) - 1; idx >= 0; idx-- {
		if rf.log[idx].Term == term {
			return idx
		}
	}
	return -1
}

func (rf *Raft) updateCommitIndexLocked() {
	for idx := len(rf.log) - 1; idx > rf.commitIndex; idx-- {
		if rf.log[idx].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= idx {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = idx
			rf.logf("Log entry committed. Updating commitIndex to %d.", rf.commitIndex)
			rf.applyCond.Broadcast()
			break
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	term := rf.currentTerm
	index := len(rf.log)
	entry := LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	rf.appendBroadcast()
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	if rf.electionTimer != nil {
		rf.resetTimer(rf.electionTimer)
	}
	if rf.heartbeatTimer != nil {
		rf.resetTimer(rf.heartbeatTimer)
	}
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// startElection initiates a new election.
func (rf *Raft) startElection() {
	rf.toCandidate()
	votes := 1
	entry := rf.lastLog()
	rf.logf("Election timer expired, starting new election. current log: %v", rf.log)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: entry.Index,
		LastLogTerm:  entry.Term,
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}
			if rf.updateTerm(reply.Term) {
				return
			}
			if reply.VoteGranted {
				rf.logf("Received vote from %d.", server)
				votes++
				if votes > len(rf.peers)/2 {
					rf.logf("Election won with %d votes.", votes)
					rf.toLeader()
				}
			}
		}(peer)
	}
}

// ticker is a long-running goroutine that checks for election timeouts
// and sends heartbeats if this server is the leader.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.startElection()
			rf.resetTimer(rf.electionTimer)
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

// applier is a long-running goroutine that applies committed log entries to the state machine.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])
		rf.logf("Applying %d from index %d to %d.", entries, lastApplied, commitIndex)
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

// Make creates a new Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	// init Raft struct
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		VotedFor:    -1,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
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
