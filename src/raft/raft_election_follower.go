package raft

//
// example RequestVote RPC handler.
// receiver implementation
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	//follower的日志更新
	if moreUpToDate(rf.getLastLogIndex(),rf.getLastLogTerm(),args.LastLogIndex,args.LastLogTerm){
		reply.VoteGranted = false
	} else if rf.votedFor == args.CandidateId || rf.votedFor == -1{
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.refreshElectionTimeout()
	}else{
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	return
}
