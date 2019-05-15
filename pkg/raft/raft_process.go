package raft

func (r *Raft) processShowStatusRequest(req ShowStatusRequest) ShowStatusResponse {
	b := ShowStatusResponse{}
	b.Term, _ = r.storage.GetCurrentTerm()
	b.CommitIndex, _ = r.storage.GetCommitIndex()
	b.Peers = r.peers
	b.State = r.state
	return b
}

func (r *Raft) processAppendEntriesRequest(req AppendEntriesRequest) AppendEntriesResponse {
	currentTerm := r.storage.MustGetCurrentTerm()

	if req.Term < currentTerm {
		return newAppendEntriesResponse(false, currentTerm, "req.Term < currentTerm")
	}

	if req.Term == currentTerm {
		if r.state == LEADER {
			return newAppendEntriesResponse(false, currentTerm, "i'm leader")
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

	lastLogIndex, _ := r.storage.MustGetLastLogIndexAndTerm()
	if req.PrevLogIndex > lastLogIndex {
		return newAppendEntriesResponse(false, currentTerm, "log not match")
	}

	// TODO: truncate earlier log
	r.storage.AppendLogEntries(req.LogEntries)
	r.storage.PutCommitIndex(req.CommitIndex)
	return newAppendEntriesResponse(true, currentTerm, "success")
}

func (r *Raft) processRequestVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	currentTerm := r.storage.MustGetCurrentTerm()
	votedFor := r.storage.MustGetVotedFor()
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()

	// if the caller's term smaller than mine, refuse
	if req.Term < currentTerm {
		return newRequestVoteResponse(false, currentTerm, lastLogIndex, "")
	}

	// if the term is equal and we've already voted for another candidate
	if req.Term == currentTerm && votedFor != "" && votedFor != req.CandidateID {
		return newRequestVoteResponse(false, currentTerm, lastLogIndex, "")
	}

	// if the caller's term bigger than my term: set currentTerm = T, convert to follower
	if req.Term > currentTerm {
		r.setState(FOLLOWER)
		r.storage.PutCurrentTerm(req.Term)
		r.storage.PutVotedFor(req.CandidateID)
	}

	// if the candidate's log is not at least as update as our last log
	if lastLogIndex > req.LastLogIndex || lastLogTerm > req.LastLogTerm {
		return newRequestVoteResponse(false, currentTerm, lastLogIndex, "")
	}

	r.storage.PutVotedFor(req.CandidateID)
	return newRequestVoteResponse(true, currentTerm, lastLogIndex, "")
}
