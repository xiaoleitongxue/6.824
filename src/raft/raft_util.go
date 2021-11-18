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
	return time.Duration(100) * time.Millisecond
}

//return random timeout between 150ms~300ms
func RandomizedElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(300)+150) * time.Millisecond
}

func max(a int, b int) int {
	if a > b{
		return a
	}else {
		return b
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// return true if log1 is strictly more up-to-date than log2
func moreUpToDate(lastLogIndex1 int, lastLogTerm1 int, lastLogIndex2 int, lastLogTerm2 int) bool {
	ans := false
	if lastLogTerm1 != lastLogTerm2 {
		ans = lastLogTerm1 > lastLogTerm2
	} else {
		ans = lastLogIndex1 > lastLogIndex2
	}
	DPrintf("[moreuptodate] %v %v , %v %v, ans=%v", lastLogIndex1, lastLogTerm1, lastLogIndex2, lastLogTerm2, ans)
	return ans
}

func (rf *Raft) refreshElectionTimeout() {
	rf.lastHeartbeat = time.Now().UnixNano() / 1e6
}

func (rf *Raft) getMajority() int32 {
	return int32((len(rf.peers) / 2) + 1)
}