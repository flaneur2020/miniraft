package raft

import (
	"fmt"
	"log"
)

type Follower struct {
	*Raft
}

func NewFollower(r *Raft) *Follower {
	f := &Follower{}
	f.Raft = r
	return f
}

func (r *Follower) Loop() {
	electionTimer := NewTimerBetween(r.electionTimeout, r.electionTimeout*2)
	for r.state == FOLLOWER {
		select {
		case <-electionTimer.C:
			log.Printf("follower.loop.electionTimeout id=%s", r.ID)
			r.setState(CANDIDATE)
		case <-r.closed:
			r.closeRaft()
		case ev := <-r.reqc:
			switch req := ev.(type) {
			case AppendEntriesRequest:
				r.respc <- r.processAppendEntriesRequest(req)
				electionTimer = NewTimerBetween(r.electionTimeout, r.electionTimeout*2)
			case RequestVoteRequest:
				r.respc <- r.processRequestVoteRequest(req)
			case ShowStatusRequest:
				r.respc <- r.processShowStatusRequest(req)
			default:
				r.respc <- ServerResponse{Code: 400, Message: fmt.Sprintf("invalid request for follower: %v", req)}
			}
		}
	}
}

func (r *Follower) processAppendEntriesRequest(req AppendEntriesRequest) AppendEntriesResponse {
	return AppendEntriesResponse{}
}

func (r *Follower) processRequestVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	return RequestVoteResponse{}
}
