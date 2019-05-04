package raft

import (
	"fmt"
)

type Candidate struct {
	*Raft
}

func NewCandidate(r *Raft) *Candidate {
	c := &Candidate{}
	c.Raft = r
	return c
}

func (r *Candidate) Loop() {
	for r.state == CANDIDATE {
		select {
		case <-r.closed:
			r.closeRaft()
		case ev := <-r.reqc:
			switch req := ev.(type) {
			case AppendEntriesRequest:
				r.respc <- r.processAppendEntriesRequest(req)
			case RequestVoteRequest:
				r.respc <- r.processRequestVoteRequest(req)
			case ShowStatusRequest:
				r.respc <- r.processShowStatusRequest(req)
			default:
				r.respc <- RaftResponse{Code: 400, Message: fmt.Sprintf("invalid request for candidate: %v", req)}
			}
		}
	}
}

func (r *Candidate) processAppendEntriesRequest(req AppendEntriesRequest) RaftResponse {
	return RaftResponse{Code: SUCCESS, Message: "success"}
}

func (r *Candidate) processRequestVoteRequest(req RequestVoteRequest) RaftResponse {
	return RaftResponse{Code: SUCCESS}
}
