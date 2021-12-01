package raft

//import "time"

func (rf *Raft) Ticker() {

	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}

	/*
		for rf.killed() == false {
			time.Sleep(StableHeartbeatTimeout())
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.refreshElectionTimeout()
				//rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	*/

}

/*
func (rf *Raft) electionTicker(){
	for rf.killed() == false {
		rf.mu.Lock()
		before := rf.lastHeartbeat
		rf.mu.Unlock()
		time.Sleep(RandomizedElectionTimeout())
		rf.mu.Lock()
		after := rf.lastHeartbeat
		if before == after && rf.state != Leader{

			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.StartElection()
			//rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.refreshElectionTimeout()
		}
		rf.mu.Unlock()
	}
}
*/
