package raft

import (
	"math/rand"
	"time"
)

type NodeState int

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(50) * time.Millisecond
}


func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	d := time.Duration(rand.Intn(300)+150) * time.Millisecond
	return d
}

func max(a int, b int) int {
	if a > b{
		return a
	}else {
		return b
	}
}

func min(a int, b int) int {
	if a > b{
		return b
	}else {
		return a
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}


func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() Entry{
	return rf.logs[0]
}

func (rf *Raft) getLog(i int) Entry{
	return rf.logs[i]
}





func (rf *Raft) getMajority() int32 {
	return int32((len(rf.peers) / 2) + 1)
}

//params is sender's
func (rf *Raft) isLogUpToDate(term int, index int) bool {
	ans := false
	if rf.getLastLogTerm() != term{
		ans = rf.getLastLogTerm() > term
	}else{
		ans = rf.getLastLogIndex() > index
	}
	return ans
}
//rf is receiver
func (rf *Raft) matchLog(prevLogTerm int, prevLogIndex int) bool{
	if prevLogIndex > rf.getLastLogIndex() || rf.logs[prevLogIndex].Term != prevLogTerm{
		return false
	}else{
		return true
	}
}

func (rf *Raft) refreshElectionTimeout(){
	//rf.lastHeartbeat = time.Now().UnixNano() /1e6
}