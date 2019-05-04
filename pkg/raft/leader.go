package raft

import (
	"fmt"
	"time"
)

type Leader struct {
	*Raft

	heartbeatTicker *time.Ticker
	peerLogIndexes  map[string]uint64
}

func NewLeader(r *Raft) *Leader {
	peerLogIndexes := map[string]uint64{}
	for id, _ := range r.peers {
		peerLogIndexes[id] = 0
	}

	l := &Leader{}
	l.Raft = r
	l.peerLogIndexes = peerLogIndexes
	l.heartbeatTicker = time.NewTicker(r.heartbeatInterval)
	return l
}

func (r *Leader) Loop() {
	for r.state == LEADER {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-r.heartbeatTicker.C:
			r.broadcastHeartbeats()
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

func (r *Leader) broadcastHeartbeats() {
}

func (r *Leader) buildLogEntriesForPeer(peerID string) []RaftLogEntry {
	return nil
}

func (r *Leader) processAppendEntriesRequest(req AppendEntriesRequest) RaftResponse {
	return RaftResponse{Code: BAD_REQUEST, Message: "i'm leader"}
}

func (r *Leader) processRequestVoteRequest(req RequestVoteRequest) RaftResponse {
	return RaftResponse{Code: SUCCESS}
}
