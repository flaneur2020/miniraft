package raft

import "log"

func (r *Raft) broadcastHeartbeats(nextLogIndexes map[string]uint64) error {
	requests, err := r.buildAppendEntriesRequests(nextLogIndexes)
	if err != nil {
		return err
	}
	for id, request := range requests {
		p := r.peers[id]
		resp, err := r.requester.SendAppendEntriesRequest(p, request)
		if err != nil {
			return err
		}
		log.Printf("raft.leader.append-entries resp=%-v", resp)
	}
	return nil
}

// requestVote broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *Raft) runElection(grantedC chan bool) error {
	_assert((r.state == CANDIDATE), "should be candidate")
	// increase candidate's term and vote for itself
	currentTerm := r.storage.MustGetCurrentTerm()
	r.storage.PutCurrentTerm(currentTerm + 1)
	r.storage.PutVotedFor(r.ID)
	log.Printf("raft.candidate.vote term=%d votedFor=%s", currentTerm, r.ID)

	// send requestVote requests asynchronously, collect the vote results into grantedC
	requests, err := r.buildRequestVoteRequests()
	if err != nil {
		return err
	}
	peers := map[string]Peer{}
	for id, p := range r.peers {
		peers[id] = p
	}
	go func() {
		granted := 0
		for id, req := range requests {
			p := peers[id]
			resp, err := r.requester.SendRequestVoteRequest(p, req)
			if err != nil {
				log.Printf("raft.candidate.send-request-vote target=%s err=%s", id, err)
				continue
			}
			if resp.VoteGranted {
				granted++
			}
		}
		log.Printf("raft.candidate.broadcast-request-vote granted=%d total=%d", granted, len(r.peers))
		if granted*2 > len(peers) {
			grantedC <- true
		} else {
			grantedC <- false
		}
	}()
	return nil
}

func (r *Raft) buildRequestVoteRequests() (map[string]*RequestVoteRequest, error) {
	le, err := r.storage.GetLastLogEntry()
	if err != nil {
		return map[string]*RequestVoteRequest{}, err
	}

	requests := map[string]*RequestVoteRequest{}
	for id := range r.peers {
		req := RequestVoteRequest{}
		req.CandidateID = r.ID
		req.LastLogIndex = le.Index
		req.LastLogTerm = le.Term
		requests[id] = &req
	}
	return requests, nil
}

func (r *Raft) buildAppendEntriesRequests(nextLogIndexes map[string]uint64) (map[string]*AppendEntriesRequest, error) {
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
