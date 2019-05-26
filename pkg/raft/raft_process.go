package raft

import "fmt"

func (r *Raft) processShowStatusRequest(req ShowStatusRequest) ShowStatusResponse {
	b := ShowStatusResponse{}
	b.Term = r.storage.MustGetCurrentTerm()
	b.CommitIndex = r.storage.MustGetCommitIndex()
	b.Peers = r.peers
	b.State = r.state
	return b
}

func (r *Raft) processAppendEntriesRequest(req AppendEntriesRequest) AppendEntriesResponse {
	currentTerm := r.storage.MustGetCurrentTerm()
	lastLogIndex, _ := r.storage.MustGetLastLogIndexAndTerm()

	if req.Term < currentTerm {
		return newAppendEntriesResponse(false, currentTerm, lastLogIndex, "req.Term < currentTerm")
	}

	if req.Term == currentTerm {
		if r.state == LEADER {
			return newAppendEntriesResponse(false, currentTerm, lastLogIndex, "i'm leader")
		}
		if r.state == CANDIDATE {
			// while waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader
			// if the leader's term is at least as large as the candidate's current term, then the candidate recognizes the leader
			// as legitimate and returns to follower state.
			r.setState(FOLLOWER)
		}
	}

	if req.Term > currentTerm {
		r.setState(FOLLOWER)
		r.storage.PutCurrentTerm(req.Term)
		r.storage.PutVotedFor("")
	}

	if req.PrevLogIndex > lastLogIndex {
		return newAppendEntriesResponse(false, currentTerm, lastLogIndex, "log not match")
	}

	if req.PrevLogIndex < lastLogIndex {
		r.storage.MustTruncateSince(req.PrevLogIndex + 1)
	}

	r.storage.AppendLogEntries(req.LogEntries)
	r.storage.PutCommitIndex(req.CommitIndex)

	lastLogIndex, _ = r.storage.MustGetLastLogIndexAndTerm()
	return newAppendEntriesResponse(true, currentTerm, lastLogIndex, "success")
}

func (r *Raft) processRequestVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	currentTerm := r.storage.MustGetCurrentTerm()
	votedFor := r.storage.MustGetVotedFor()
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()

	// if the caller's term smaller than mine, refuse
	if req.Term < currentTerm {
		return newRequestVoteResponse(false, currentTerm, "")
	}

	// if the term is equal and we've already voted for another candidate
	if req.Term == currentTerm && votedFor != "" && votedFor != req.CandidateID {
		return newRequestVoteResponse(false, currentTerm, "")
	}

	// if the caller's term bigger than my term: set currentTerm = T, convert to follower
	if req.Term > currentTerm {
		r.setState(FOLLOWER)
		r.storage.PutCurrentTerm(req.Term)
		r.storage.PutVotedFor(req.CandidateID)
	}

	// if the candidate's log is not at least as update as our last log
	if lastLogIndex > req.LastLogIndex || lastLogTerm > req.LastLogTerm {
		return newRequestVoteResponse(false, currentTerm, "")
	}

	r.storage.PutVotedFor(req.CandidateID)
	return newRequestVoteResponse(true, currentTerm, "")
}

func (r *Raft) processCommandRequest(req CommandRequest) CommandResponse {
	switch req.Command.OpType {
	case kNop:
		return CommandResponse{Value: []byte{}, Message: "nop"}
	case kPut:
		logIndex, _ := r.storage.AppendLogEntriesByCommands([]RaftCommand{req.Command})
		// TODO: await logIndex got commit
		return CommandResponse{Value: []byte{}, Message: fmt.Sprintf("logIndex: %d", logIndex)}
	case kGet:
		v, exists := r.storage.MustGetKv(req.Command.Key)
		if !exists {
			return CommandResponse{Value: nil, Message: "not found"}
		}
		return CommandResponse{Value: v, Message: "success"}
	default:
		panic(fmt.Sprintf("unexpected opType: %s", req.Command.OpType))
	}
}
