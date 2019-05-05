package raft

import (
	"fmt"
	"log"
)

type Candidate struct {
	*Raft
}

func NewCandidate(r *Raft) *Candidate {
	c := &Candidate{}
	c.Raft = r
	return c
}

// After a candidate raise a rote:
// 它自己赢得选举；
// 另一台机器宣称自己赢得选举；
// 一段时间过后没有赢家
func (r *Candidate) Loop() {
	grantedC := make(chan bool)
	electionTimer := NewTimerBetween(r.electionTimeout, r.electionTimeout*2)
	r.runElection(grantedC)
	for r.state == CANDIDATE {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-electionTimer.C:
			r.runElection(grantedC)
			electionTimer = NewTimerBetween(r.electionTimeout, r.electionTimeout*2)
		case granted := <-grantedC:
			if granted {
				r.upgradeToLeader()
				continue
			} else {

			}
		case ev := <-r.reqc:
			switch req := ev.(type) {
			case RequestVoteRequest:
				r.respc <- r.processRequestVoteRequest(req)
			case ShowStatusRequest:
				r.respc <- r.processShowStatusRequest(req)
			default:
				r.respc <- ServerResponse{Code: 400, Message: fmt.Sprintf("invalid request for candidate: %v", req)}
			}
		}
	}
}

// processAppendEntriesRequest maybe receives the appendEntries from the new leader.
// > While waiting for votes, a candidate may receive an AppendEntries RPC from another server
// > claiming to be leader. If the leader’s term (included in its RPC) is at least as large
// > as the candidate’s current term, then the candidate recognizes the leader as legitimate
// > and returns to follower state.
func (r *Candidate) processAppendEntriesRequest(req AppendEntriesRequest) AppendEntriesResponse {
	currentTerm := r.storage.MustGetCurrentTerm()
	if req.Term > currentTerm {
		r.setState(FOLLOWER)
		return newAppendEntriesResponse(true, currentTerm)
	}
	return newAppendEntriesResponse(false, currentTerm)
}

func (r *Candidate) processRequestVoteRequest(req RequestVoteRequest) RequestVoteResponse {
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

// runElection broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *Candidate) runElection(grantedC chan bool) error {
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
			resp, err := p.SendRequestVoteRequest(req)
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

func (r *Candidate) upgradeToLeader() {
	r.setState(LEADER)
}

func (r *Candidate) buildRequestVoteRequests() (map[string]*RequestVoteRequest, error) {
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
