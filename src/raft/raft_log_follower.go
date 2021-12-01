package raft

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//1
	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.ChangeState(Follower)
	//rf.refreshElectionTimeout()
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	////check logs
	////too old request
	//if request.PrevLogIndex < rf.getFirstLog().Index{
	//	response.Term, response.Success = 0, false
	//	return
	//}

	//2
	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {

		lastIndex := rf.getLastLog().Index
		//if you received a prc which prevLogIndex beyond end of your logs
		if lastIndex < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
			response.Term, response.Success = rf.currentTerm, false
		} else {
			response.ConflictTerm, response.ConflictIndex = -1, request.PrevLogIndex
			response.Term, response.Success = rf.currentTerm, false
		}
		return
	}


	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...)
			break
		}
	}

	////append entries
	//if len(request.Entries) > 0 {
	//	newLog := make([]Entry, 0)
	//	for i := rf.logs[0].Index; i <= request.PrevLogIndex; i++ {
	//		newLog = append(newLog, rf.logs[i])
	//	}
	//	newLog = append(newLog, request.Entries...)
	//	if !rf.isLogUpToDate(newLog[len(newLog)-1].Term, newLog[len(newLog)-1].Index) {
	//		rf.logs = newLog
	//	}
	//}

	//update commitIndex
	rf.advanceCommitIndexForFollower(request.LeaderCommit)
	//fmt.Printf("%v is successfully append logs, and logs is %v. commitIndex is %v\n",rf.me,rf.logs,rf.commitIndex)
	response.Term = rf.currentTerm
	response.Success = true
	return

}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommitIndex int) {
	oldCommitIndex := rf.commitIndex
	//
	newCommitIndex := min(leaderCommitIndex, rf.getLastLogIndex())

	if oldCommitIndex < newCommitIndex {
		for i := oldCommitIndex + 1; i <= newCommitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
		}
		rf.commitIndex = newCommitIndex
	}
}


