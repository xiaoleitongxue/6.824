package raft

// receiver implementation
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextTryIndex = -1
		return
	}
	reply.Success = true
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.refreshElectionTimeout()
	return
}