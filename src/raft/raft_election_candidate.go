package raft

import "fmt"

func (rf *Raft) StartElection() {
	args := rf.genRequestVoteRequest()
	// use Closure
	grantedVotes := 1
	rf.votedFor = rf.me
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers) /2 {
							rf.ChangeState(Leader)
							fmt.Printf("now %v becomes to leader,its term is %v\n",rf.me,rf.currentTerm)
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	return args
}
