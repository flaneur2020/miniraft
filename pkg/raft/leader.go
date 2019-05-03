package raft

import "fmt"

type Leader struct {
	*Raft
}

func NewLeader(r *Raft) *Leader {
	l := &Leader{}
	l.Raft = r
	return l
}

func (r *Leader) Loop() {
	for r.state == LEADER {
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
				r.respc <- RaftResponse{Code: 400, Message: fmt.Sprintf("invalid request: %v", req)}
			}
		}
	}
}

func (r *Leader) processAppendEntriesRequest(req AppendEntriesRequest) RaftResponse {
	return RaftResponse{Code: SUCCESS, Message: "success"}
}

func (r *Leader) processRequestVoteRequest(req RequestVoteRequest) RaftResponse {
	return RaftResponse{Code: SUCCESS}
}
