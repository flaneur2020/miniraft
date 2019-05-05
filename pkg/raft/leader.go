package raft

import (
	"fmt"
	"log"
	"time"
)

type Leader struct {
	*Raft

	peerPrevLogIndexes map[string]uint64
}

func NewLeader(r *Raft) *Leader {
	peerPrevLogIndexes := map[string]uint64{}
	for id, _ := range r.peers {
		peerPrevLogIndexes[id] = 0
	}

	l := &Leader{}
	l.Raft = r
	l.peerPrevLogIndexes = peerPrevLogIndexes
	return l
}

func (r *Leader) Loop() {
	heartbeatTicker := time.NewTicker(r.heartbeatInterval)
	for r.state == LEADER {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-heartbeatTicker.C:
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
				r.respc <- ServerResponse{Code: 400, Message: fmt.Sprintf("invalid request for leader: %v", req)}
			}
		}
	}
}

func (r *Leader) processAppendEntriesRequest(req AppendEntriesRequest) AppendEntriesResponse {
	return AppendEntriesResponse{}
}

func (r *Leader) processRequestVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	return RequestVoteResponse{}
}

func (r *Leader) broadcastHeartbeats() error {
	requests, err := r.buildAppendEntriesRequests()
	if err != nil {
		return err
	}
	for id, request := range requests {
		p := r.peers[id]
		resp, err := p.SendAppendEntriesRequest(request)
		if err != nil {
			return err
		}
		log.Printf("raft.leader.append-entries resp=%-v", resp)
	}
	return nil
}

func (r *Leader) buildAppendEntriesRequests() (map[string]*AppendEntriesRequest, error) {
	requests := map[string]*AppendEntriesRequest{}
	for id, idx := range r.peerPrevLogIndexes {
		logEntries, err := r.storage.GetLogEntriesSince(idx)
		if err != nil {
			return nil, err
		}
		request := &AppendEntriesRequest{}
		request.LeaderID = r.ID
		request.PrevLogIndex = 0
		request.PrevLogTerm = 0
		request.LogEntries = []RaftLogEntry{}
		request.Term, _ = r.storage.GetCurrentTerm()
		request.CommitIndex, _ = r.storage.GetCommitIndex()
		if len(logEntries) >= 1 {
			request.PrevLogIndex = logEntries[0].Index
			request.PrevLogTerm = logEntries[0].Term
		}
		if len(logEntries) >= 2 {
			request.LogEntries = logEntries[1:]
		}
		requests[id] = request
	}
	return requests, nil
}
