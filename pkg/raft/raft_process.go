package raft

func (r *Raft) processShowStatusRequest(req ShowStatusRequest) ShowStatusResponse {
	b := ShowStatusResponse{}
	b.Term, _ = r.storage.GetCurrentTerm()
	b.CommitIndex, _ = r.storage.GetCommitIndex()
	b.Peers = r.peers
	b.State = r.state
	return b
}

// processAppendEntriesRequest maybe receives the appendEntries from the new leader.
// > While waiting for votes, a candidate may receive an AppendEntries RPC from another server
// > claiming to be leader. If the leader’s term (included in its RPC) is at least as large
// > as the candidate’s current term, then the candidate recognizes the leader as legitimate
// > and returns to follower state.
func (r *Raft) processAppendEntriesRequest(req AppendEntriesRequest) AppendEntriesResponse {
	currentTerm := r.storage.MustGetCurrentTerm()
	if req.Term > currentTerm {
		r.setState(FOLLOWER)
		return newAppendEntriesResponse(true, currentTerm)
	}
	return newAppendEntriesResponse(false, currentTerm)
}

func (r *Raft) processRequestVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	currentTerm := r.storage.MustGetCurrentTerm()
	votedFor := r.storage.MustGetVotedFor()
	// lastLogEntry := r.storage.MustGetLastLogEntry()

	// if the caller's term smaller than mine, refuse
	if req.Term < currentTerm {
		return newRequestVoteResponse(false, currentTerm, "")
	}

	// if the caller's term bigger than mine: set currentTerm = T, convert to follower
	if req.Term > currentTerm {
		r.setState(FOLLOWER)
		r.storage.PutCurrentTerm(req.Term)
		r.storage.PutVotedFor(req.CandidateID)
	}

	// if votedFor is empty or candidateID, and the candidate's log is at least up-to-date as my log, grant vote
	if votedFor == "" || votedFor == req.CandidateID {
		// TODO
		return newRequestVoteResponse(false, currentTerm, "")
	}
	return newRequestVoteResponse(false, currentTerm, "")
}
