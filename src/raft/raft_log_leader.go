package raft

func (rf *Raft) sendHeartbeat(peer int,startTerm int, prevLogIndex int, prevLogTerm int, entries []Entry, leaderCommitIndex int){
	args := &AppendEntriesArgs{
		Term:              startTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommit: leaderCommitIndex,
	}
	reply := &AppendEntriesReply{}
	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v, args=%+v", rf.me, peer, args)
	if ok := rf.sendAppendEntries(peer, args, reply); !ok {
		DPrintf("[sendHeartbeat] leader %v send to %v rpc error", rf.me, peer)
		return
	}
	//handle peer's reply after send
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//term changed after we rehold the lock
	if startTerm != rf.currentTerm{
		return
	}
	//follower reply
	if reply.Success == false{
		if reply.Term > startTerm{
			rf.currentTerm = max(reply.Term,startTerm)
			rf.state = Follower
		} else if reply.NextTryIndex > 0{
			rf.nextIndex[peer] = reply.NextTryIndex
		}
		rf.persist()
		return
	}
}


