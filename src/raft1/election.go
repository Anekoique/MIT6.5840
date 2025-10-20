package raft

// the election logic for Raft.

// startElection initiates a new election.
func (rf *Raft) startElection() {
	rf.logf("Election timer expired, starting new election.")
	rf.toCandidate()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLog().Index,
		LastLogTerm:  rf.lastLog().Term,
	}

	votes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply); !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.isStateBehind(reply.Term, Candidate, args.Term) {
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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

		rf.logf("Granted: %v vote to candidate %d.", granted, args.CandidateID)
		if granted {
			rf.VotedFor = args.CandidateID
			reply.VoteGranted = true
			rf.resetTimer(rf.electionTimer)
			rf.persist(nil)
		}
	}
}
