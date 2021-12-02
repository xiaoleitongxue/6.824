package raft

//rf is receiver
func (rf *Raft) RequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if request.Term < rf.currentTerm {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	// if receiver's log more update to date
	if rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.VoteGranted = false
	}else if rf.votedFor == request.CandidateId || rf.votedFor == -1{
		rf.votedFor = request.CandidateId
		rf.currentTerm = request.Term
		response.VoteGranted = true
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	}else{
		response.VoteGranted = false
	}
	response.Term = rf.currentTerm
	return
}
