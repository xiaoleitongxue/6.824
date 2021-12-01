package raft

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm{
		rf.currentTerm,rf.votedFor = request.Term,-1
	}


	rf.ChangeState(Follower)
	rf.refreshElectionTimeout()
	//rf.electionTimer.Reset(RandomizedElectionTimeout())

	//check logs
	//too old request
	if request.PrevLogIndex < rf.getFirstLog().Index{
		response.Term, response.Success = 0, false
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
		} else {
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index
		}
		return
	}

	//firstIndex := rf.getFirstLog().Index
	//for index, entry := range request.Entries {
	//	if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
	//		rf.logs = append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...)
	//		break
	//	}
	//}

	//

	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...)
			break
		}
	}

	//update commitIndex
	rf.advanceCommitIndexForFollower(request.LeaderCommit)
	response.Term = rf.currentTerm
	response.Success = true
	return

}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommitIndex int) {
	oldCommitIndex := rf.commitIndex
	newCommitIndex := min(leaderCommitIndex, rf.getLastLogIndex())

	if oldCommitIndex < newCommitIndex{
		for i := oldCommitIndex + 1; i<=newCommitIndex;i++{
			msg := ApplyMsg{
				CommandValid: true,
				Command: rf.logs[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
		}
		rf.commitIndex = newCommitIndex
	}
	return
}