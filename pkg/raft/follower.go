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
				log.Printf("follower.loop reqc=AppendEntriesRequest req=%-v", req)
				resp := r.processAppendEntriesRequest(req)
				r.respc <- resp
				r.resetElectionTimer()
			case RequestVoteRequest:
				resp := r.processRequestVoteRequest(req)
				r.respc <- resp
			default:
				r.respc <- RaftResponse{Code: 500, Message: fmt.Sprintf("unknown message: %v", req)}
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
