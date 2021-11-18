package raft

func (rf *Raft) startElection(startTerm int) {
	rf.mu.Lock()

	//////在开始选举时，rf的状态可能会改变，需要重新判断
	//if rf.currentTerm != startTerm || rf.state != Candidate {
	//	rf.mu.Unlock()
	//	return
	//}
	rf.votedFor = rf.me
	grantedVotes := 1
	rf.persist()
	args := RequestVoteArgs{
		Term : rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.logs[len(rf.logs) - 1].Index,
		LastLogTerm: rf.logs[len(rf.logs) - 1].Term,
	}
	rf.mu.Unlock()

	for peer := range rf.peers{
		if peer == rf.me{
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(peer, &args, &reply);!ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm == args.Term && rf.state == Candidate{
				if reply.VoteGranted {
					grantedVotes += 1
					if grantedVotes > len(rf.peers)/2 {
						rf.ChangeState(Leader)
						rf.leaderInit()
					}
				} else if reply.Term > rf.currentTerm {
					rf.ChangeState(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				}
			}
		}(peer)
	}
}

func (rf *Raft) leaderInit(){
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.nextIndex{
		rf.nextIndex[peer] = rf.logs[len(rf.logs) - 1].Index + 1
	}
}