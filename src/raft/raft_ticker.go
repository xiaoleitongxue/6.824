package raft

import "time"

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// every peer have a ticker of his own
func (rf *Raft) electionTicker() {

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		before := rf.lastHeartbeat
		rf.mu.Unlock()
		time.Sleep(RandomizedElectionTimeout())

		rf.mu.Lock()
		after := rf.lastHeartbeat
		state := rf.state
		//在选举超时这段时间内没有收到心跳，转为候选者
		if before == after && state != Leader{
			rf.currentTerm++
			rf.state = Candidate
			startTerm := rf.currentTerm
			go rf.startElection(startTerm)
		}
		rf.mu.Unlock()
	}
}


func (rf *Raft) leaderTicker(){
	for rf.killed() == false{
		//leader间隔一定的时间发送心跳
		time.Sleep(StableHeartbeatTimeout())
		rf.leaderHandler()
	}
}

func (rf *Raft) leaderHandler(){
	rf.mu.Lock()
	state := rf.state
	term := rf.currentTerm
	if state == Leader{
		for peer := range rf.peers{
			if peer == rf.me{
				continue
			}
			prevLogIndex := rf.nextIndex[peer] - 1
			prevLogTerm := rf.logs[prevLogIndex].Term
			entries := make([]Entry,0)
			//准备要附加的日志
			for j := prevLogIndex + 1; j <= rf.getLastLogIndex(); j++ {
				entries = append(entries, rf.logs[j])
			}
			leaderCommitIndex := rf.commitIndex
			//开n-1个线程发送日志追加请求
			go rf.sendHeartbeat(peer,term, prevLogIndex, prevLogTerm, entries, leaderCommitIndex)
		}
	}
	rf.mu.Unlock()
}
