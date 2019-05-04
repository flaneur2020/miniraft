package raft

import (
	"fmt"
	"log"
	"time"
)

type Follower struct {
	*Raft

	electionTimer *time.Timer
}

func NewFollower(r *Raft) *Follower {
	f := &Follower{}
	f.Raft = r
	return f
}

func (r *Follower) Loop() {
	r.resetElectionTimer()
	for r.state == FOLLOWER {
		select {
		case <-r.electionTimer.C:
			log.Printf("follower.loop.electionTimeout id=%s", r.ID)
			r.upgradeToCandidate()
		case <-r.closed:
			r.closeRaft()
		case ev := <-r.reqc:
			switch req := ev.(type) {
			case AppendEntriesRequest:
				r.respc <- r.processAppendEntriesRequest(req)
				r.resetElectionTimer()
			case RequestVoteRequest:
				r.respc <- r.processRequestVoteRequest(req)
			case ShowStatusRequest:
				r.respc <- r.processShowStatusRequest(req)
			default:
				r.respc <- RaftResponse{Code: 400, Message: fmt.Sprintf("invalid request: %v", req)}
			}
		}
	}
}

func (r *Follower) processAppendEntriesRequest(req AppendEntriesRequest) RaftResponse {
	return RaftResponse{Code: SUCCESS, Message: "success"}
}

func (r *Follower) processRequestVoteRequest(req RequestVoteRequest) RaftResponse {
	return RaftResponse{Code: SUCCESS}
}

func (r *Follower) upgradeToCandidate() {
	r.setState(CANDIDATE)
}

func (r *Follower) resetElectionTimer() {
	if r.electionTimer == nil {
		r.electionTimer = time.NewTimer(r.electionTimeout)
	}
	r.electionTimer.Reset(r.electionTimeout)
}
