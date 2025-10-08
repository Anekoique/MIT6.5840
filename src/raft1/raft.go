package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type (
	RaftState int
)

const (
	Follower RaftState = iota
	Candidate
	Leader
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

const (
	HeartbeatInterval = 100 * time.Millisecond
	ElectionInterval  = 600 * time.Millisecond
	RPCTimeout        = 50 * time.Millisecond
)

// Raft is A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *tester.Persister   // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan raftapi.ApplyMsg
	applyCond      *sync.Cond

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
}

// logf formats a log message with a standard prefix.
// The caller MUST hold rf.mu.
func (rf *Raft) logf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[%d][T%d][%s] ", rf.me, rf.currentTerm, rf.state.String())
	format = prefix + format
	DPrintf(format, a...)
}

type LogEntry struct {
	Term    int
	Command any
	Index   int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) atomicAdd(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logf("Incrementing nextIndex for peer %d to %d", server, rf.nextIndex[server]+1)
	rf.nextIndex[server] = min(rf.nextIndex[server]+1, len(rf.log))
}

func (rf *Raft) atomicSub(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logf("Decrementing nextIndex for peer %d to %d", server, rf.nextIndex[server]-1)
	rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()

	rf.state = Leader
	index := len(rf.log)
	for i := range rf.peers {
		rf.nextIndex[i] = index
		rf.matchIndex[i] = 0
	}
	rf.logf("Transitioned to Leader. Initializing nextIndex for all peers to %d.", index)
	rf.heartbeatTimer.Reset(HeartbeatInterval)
	rf.mu.Unlock()
	rf.appendBroadcast(true)
}

func (rf *Raft) toCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.VotedFor = rf.me
	rf.logf("Transitioned to Candidate, starting election for new term.")
}

func (rf *Raft) toFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.logf("Transitioned to Follower.")
}

func (rf *Raft) checkTerm(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return term == rf.currentTerm
}

func (rf *Raft) updateTerm(newTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if newTerm > rf.currentTerm {
		rf.logf("Discovered a newer term %d (our term is %d). Transitioning to Follower.", newTerm, rf.currentTerm)
		rf.currentTerm = newTerm
		rf.VotedFor = -1
		rf.state = Follower
		return true
	}
	return false
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

func (rf *Raft) isCondidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Candidate
}

func (rf *Raft) isFollower() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Follower
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	rf.updateTerm(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term == rf.currentTerm && rf.VotedFor == -1 {
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term

		logOk := args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		if logOk {
			rf.VotedFor = args.CandidateID
			reply.VoteGranted = true
			rf.electionTimer.Reset(rf.getTimeout())
			rf.logf("Granted vote to candidate %d.", args.CandidateID)
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(ch chan RequestVoteReply) {
	rf.mu.Lock()
	index := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  rf.log[index].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			ch <- reply
		}(i)
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.updateTerm(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		rf.logf("Rejected AppendEntries from %d: sender's term %d is stale (our term is %d).", args.LeaderID, args.Term, rf.currentTerm)
		return
	}
	if args.PrevLogIndex >= len(rf.log) {
		reply.Term, reply.Success = rf.currentTerm, false
		rf.logf("Rejected AppendEntries from %d: PrevLogIndex %d is out of bounds (our log len is %d).", args.LeaderID, args.PrevLogIndex, len(rf.log))
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		rf.logf("Rejected AppendEntries from %d: Term mismatch at PrevLogIndex %d.", args.LeaderID, args.PrevLogIndex)
		return
	}
	if len(args.Entries) != 0 {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		rf.logf("Appended %d entries from leader %d.", len(args.Entries), args.LeaderID)
	}

	reply.Term, reply.Success = rf.currentTerm, true
	rf.state = Follower
	rf.electionTimer.Reset(rf.getTimeout())
	if args.LeaderCommit > rf.commitIndex && args.LeaderCommit < len(rf.log) {
		rf.commitIndex = args.LeaderCommit
		rf.logf("Updated commitIndex to %d.", rf.commitIndex)
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) appendOnce(server int, isReplicate bool) bool {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}
	index := rf.nextIndex[server] - 1
	rf.logf("Sending AppendEntries to peer %d (isReplicate: %v, prevLogIndex: %v).", server, isReplicate, index)
	term := rf.log[index].Term
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: index,
		PrevLogTerm:  term,
		LeaderCommit: rf.commitIndex,
	}
	if isReplicate {
		args.Entries = make([]LogEntry, len(rf.log)-index-1)
		copy(args.Entries, rf.log[index+1:])
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	done := make(chan bool)
	var callResult bool
	go func() {
		callResult = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		done <- true
	}()

	select {
	case <-done:
		if !callResult {
			return false
		}
		if !reply.Success && !rf.updateTerm(reply.Term) && rf.checkTerm(args.Term) {
			DPrintf("[%d] AppendEntries to %v failed, retrying.", rf.me, server)
			rf.atomicSub(server)
			rf.appendOnce(server, true)
		}
		if isReplicate {
			rf.atomicAdd(server)
		}
		return true
	case <-time.After(RPCTimeout):
		DPrintf("[%d] RPC timeout sending AppendEntries to server %v", rf.me, server)
		return false
	}
}

func (rf *Raft) appendBroadcast(isReplicate bool) {
	var wg sync.WaitGroup
	counter := int32(1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			if rf.appendOnce(server, isReplicate) {
				atomic.AddInt32(&counter, 1)
			}
		}(i)
	}

	go func() {
		wg.Wait()
		if !rf.isLeader() {
			return
		}
		if int(atomic.LoadInt32(&counter)) < len(rf.peers)/2+1 {
			DPrintf("[%d] Stepping down, unable to reach majority for AppendEntries.", rf.me)
			rf.toFollower()
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if isReplicate {
			rf.logf("Log entry committed. Updating commitIndex to %d.", len(rf.log)-1)
			rf.commitIndex = len(rf.log) - 1
			rf.applyCond.Broadcast()
		}
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	}()
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
	if !rf.isLeader() {
		return -1, -1, false
	}

	rf.mu.Lock()
	term := rf.currentTerm
	index := len(rf.log)
	entry := LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	}
	rf.log = append(rf.log, entry)
	rf.mu.Unlock()
	rf.appendBroadcast(true)
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.toCandidate()
	voteCh := make(chan RequestVoteReply)
	rf.sendRequestVote(voteCh)

	timer := time.After(ElectionInterval)
	response := 1
	votes := 1
	for response < len(rf.peers) {
		if !rf.isCondidate() {
			return
		}

		select {
		case reply := <-voteCh:
			response++
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					DPrintf("[%d] Election won with %d votes.", rf.me, votes)
					rf.toLeader()
					return
				}
			}
		case <-timer:
			DPrintf("[%d] Election timed out.", rf.me)
			rf.toFollower()
			return
		}
	}
	rf.toFollower()
}

func (rf *Raft) getTimeout() time.Duration {
	ms := 600 + (rand.Int63() % 400)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			if rf.isFollower() {
				DPrintf("[%v] Election timer expired, starting new election.", rf.me)
				rf.startElection()
				rf.mu.Lock()
				rf.electionTimer.Reset(rf.getTimeout())
				rf.mu.Unlock()
			}
		case <-rf.heartbeatTimer.C:
			if rf.isLeader() {
				DPrintf("[%v] Heartbeat timer expired, sending heartbeats.", rf.me)
				rf.appendBroadcast(false)
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])
		rf.logf("Applying %d entries from index %d to %d.", len(entries), lastApplied+1, commitIndex)
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

// Make create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
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
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.electionTimer = time.NewTimer(rf.getTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.log = append(rf.log, LogEntry{0, nil, 0})
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go rf.applier()

	return rf
}
