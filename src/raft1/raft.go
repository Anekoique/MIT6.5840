package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
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
	Heartbeat struct{}
)

const (
	Follower RaftState = iota
	Candidate
	Leader
)

const HeartbeatInterval = 100 * time.Millisecond
const ElectionInterval = 5 * time.Second

// Raft is A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *tester.Persister   // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	heartbeatCh chan Heartbeat
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
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

type LogEntry struct {
	Term int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) upgrade() {
	if rf.state < 2 {
		rf.state += 1
	}
}

func (rf *Raft) downgrade(level RaftState) {
	if rf.state > 0 {
		rf.state -= level
	}
}

func (rf *Raft) updateTerm(newTerm int) {
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.VotedFor = -1
		rf.state = Follower
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.state = Follower
	}
	
	reply.Term = rf.currentTerm
	if rf.VotedFor == -1 && args.Term >= rf.currentTerm {
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	DPrintf("%v vote %v: %v %v %v", rf.me, args.CandidateID, args.Term, rf.currentTerm, reply.VoteGranted)
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
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
			}
			rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
	} else {
		reply.Term, reply.Success = rf.currentTerm, true
		rf.updateTerm(args.Term)
		rf.state = Follower
		
		select {
		case rf.heartbeatCh <- Heartbeat{}:
		default:
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	DPrintf("%v sendHeartbeat", rf.me)
	responseCh := make(chan bool, len(rf.peers)-1)
	
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}
		rf.mu.Unlock()

		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			if ok && !reply.Success {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
				}
				rf.mu.Unlock()
			}
			responseCh <- ok
		}(i)
	}
	
	go func() {
		successCount := 1
		timeout := time.After(HeartbeatInterval / 2)
		for i := 0; i < len(rf.peers)-1; i++ {
			select {
			case ok := <-responseCh:
				if ok {
					successCount++
				}
			case <-timeout:
				if successCount <= len(rf.peers)/2 {
					rf.mu.Lock()
					if rf.state == Leader {
						rf.state = Follower
						DPrintf("Leader %v stepping down - unable to reach majority", rf.me)
					}
					rf.mu.Unlock()
				}
				return
			}
		}
		if successCount <= len(rf.peers)/2 {
			rf.mu.Lock()
			if rf.state == Leader {
				rf.state = Follower
				DPrintf("Leader %v stepping down - unable to reach majority", rf.me)
			}
			rf.mu.Unlock()
		}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.VotedFor = rf.me
	rf.upgrade()
	rf.mu.Unlock()

	voteCh := make(chan RequestVoteReply)
	rf.sendRequestVote(voteCh)

	timer := time.After(ElectionInterval)
	response := 0
	votes := 1
	for response < len(rf.peers)-1 {
		select {
		case reply := <-voteCh:
			rf.mu.Lock()

			response++
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					DPrintf("%v Become Leader!", rf.me)
					rf.upgrade()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			} else {
				rf.updateTerm(reply.Term)
				rf.downgrade(1)
				rf.mu.Unlock()
				DPrintf("Election failed: Downgrade! %v", rf.me)
				return
			}
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.downgrade(1)
			DPrintf("Election failed: Leader exists! %v", rf.me)
			return
		case <-timer:
			DPrintf("Election failed: TimeOut! %v", rf.me)
			rf.startElection()
		}
	}
}

func (rf *Raft) getTimeout() time.Duration {
	ms := 600 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	electionTimer := time.NewTimer(rf.getTimeout())
	heartbeatTimer := time.NewTimer(HeartbeatInterval)

	for !rf.killed() {
		select {
		case <-rf.heartbeatCh:
			DPrintf("reset Heartbeattimer %v", rf.me)
			electionTimer.Reset(rf.getTimeout())
		case <-electionTimer.C:
			if rf.isFollower() {
				DPrintf("Timeout! startElection! %v", rf.me)
				rf.startElection()
			}
			electionTimer.Reset(rf.getTimeout())
		case <-heartbeatTimer.C:
			if rf.isLeader() {
				rf.sendHeartbeat()
			}
			heartbeatTimer.Reset(HeartbeatInterval)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.heartbeatCh = make(chan Heartbeat)
	rf.VotedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
