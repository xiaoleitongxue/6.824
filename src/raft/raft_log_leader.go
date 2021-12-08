package raft

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			//fmt.Printf("%v is leader, it begins to send logs, leader's commitIndx is %v\n",rf.me,rf.commitIndex)
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//when leader's last log index > matchIndex of one peer
	//leader needs to copy logs
	return rf.state == Leader && rf.nextIndex[peer] <= rf.getLastLog().Index
}

func (rf *Raft) applier() {

}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	// just entries can catch up
	request := rf.genAppendEntriesRequest(prevLogIndex)
	rf.mu.RUnlock()
	response := new(AppendEntriesReply)
	if rf.sendAppendEntries(peer, &request, response) {
		rf.mu.Lock()
		rf.handleAppendEntriesResponse(peer, &request, response)
		rf.mu.Unlock()
	}

}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getLog(prevLogIndex).Index,
		PrevLogTerm:  rf.getLog(prevLogIndex).Term,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.logs[prevLogIndex+1:],
	}
	return args
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {

	if response.Success == false {
		if response.Term > rf.currentTerm {
			rf.currentTerm = response.Term
			rf.ChangeState(Follower)
		} else {
			rf.nextIndex[peer] = rf.nextIndex[peer] - 1
		}
		return
	}

	////too old request
	//if request.PrevLogIndex + len(request.Entries) < rf.matchIndex[peer]{
	//	return
	//}

	//if reply true, update matchIndex and nextIndex
	rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1

	//check whether leader can update its commitIndex, when logs copied to majority server, leader can
	//update its commitIndex
	oldCommitIndex := rf.commitIndex
	newCommitIndex := rf.matchIndex[peer]

	if oldCommitIndex >= newCommitIndex || rf.logs[newCommitIndex].Term != rf.currentTerm {
		return
	}
	//new
	count := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[peer] >= newCommitIndex {
			count+=1
		}
	}

	if count > len(rf.peers)/2 {
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
