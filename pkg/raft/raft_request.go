package raft

func (r *raft) broadcastHeartbeats() error {
	requests, err := r.buildAppendEntriesRequests(r.nextLogIndexes)
	if err != nil {
		return err
	}

	r.logger.Debugf("leader.broadcast-heartbeats requests=%v", requests)
	for id, request := range requests {
		p := r.peers[id]
		_, err := r.requester.SendAppendEntriesRequest(p, request)

		// TODO: 增加回退 nextLogIndex 逻辑
		if err != nil {
			return err
		}
	}
	return nil
}

// requestVote broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *raft) runElection() bool {
	if r.state != CANDIDATE  {
		panic("should be candidate")
	}

	// increase candidate's term and vote for itself
	currentTerm := r.storage.MustGetCurrentTerm()
	r.storage.PutCurrentTerm(currentTerm + 1)
	r.storage.PutVotedFor(r.ID)
	r.logger.Debugf("raft.candidate.vote term=%d votedFor=%s", currentTerm, r.ID)

	// send requestVote requests asynchronously, collect the vote results into grantedC
	requests, err := r.buildRequestVoteRequests()
	if err != nil {
		r.logger.Debugf("raft.candidate.vote.buildRequestVoteRequests err=%s", err)
		return false
	}

	peers := map[string]Peer{}
	for id, p := range r.peers {
		peers[id] = p
	}

	granted := 0
	for id, req := range requests {
		p := peers[id]
		resp, err := r.requester.SendRequestVoteRequest(p, req)
		r.logger.Debugf("raft.candidate.send-request-vote target=%s resp=%#v err=%s", id, resp, err)
		if err != nil {
			continue
		}
		if resp.VoteGranted {
			granted++
		}
	}

	success := (granted+1)*2 > len(peers)+1
	r.logger.Debugf("raft.candidate.broadcast-request-vote granted=%d total=%d success=%d", granted+1, len(r.peers)+1, success)
	return success
}

func (r *raft) buildRequestVoteRequests() (map[string]*RequestVoteRequest, error) {
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()
	currentTerm := r.storage.MustGetCurrentTerm()

	requests := map[string]*RequestVoteRequest{}
	for id := range r.peers {
		req := RequestVoteRequest{}
		req.CandidateID = r.ID
		req.LastLogIndex = lastLogIndex
		req.LastLogTerm = lastLogTerm
		req.Term = currentTerm
		requests[id] = &req
	}
	return requests, nil
}

func (r *raft) buildAppendEntriesRequests(nextLogIndexes map[string]uint64) (map[string]*AppendEntriesRequest, error) {
	requests := map[string]*AppendEntriesRequest{}
	for id, idx := range nextLogIndexes {
		request := &AppendEntriesRequest{}
		request.LeaderID = r.ID
		request.LogEntries = []RaftLogEntry{}
		request.Term = r.storage.MustGetCurrentTerm()
		request.CommitIndex = r.storage.MustGetCommitIndex()

		if idx == 0 {
			request.PrevLogIndex = 0
			request.PrevLogTerm = 0
		} else {
			logEntries := r.storage.MustGetLogEntriesSince(idx - 1)
			if len(logEntries) >= 1 {
				request.PrevLogIndex = logEntries[0].Index
				request.PrevLogTerm = logEntries[0].Term
			}
			if len(logEntries) >= 2 {
				request.LogEntries = logEntries[1:]
			}
		}
		requests[id] = request
	}
	return requests, nil
}
