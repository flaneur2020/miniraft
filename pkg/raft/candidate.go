package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
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
	grantedC := make(chan bool)
	r.runVote(grantedC)
	electionTimer := TimerBetween(r.electionTimeout, r.electionTimeout*2)
	for r.state == CANDIDATE {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-electionTimer.C:
			r.runVote(grantedC)
		case granted := <-grantedC:
			if granted {
				r.upgradeToLeader()
			} else {
				electionTimer = TimerBetween(r.electionTimeout, r.electionTimeout*2)
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
	if req.Term >= currentTerm {
		r.followTerm(req.Term)
		return newAppendEntriesResponse(true, currentTerm)
	}
	return newAppendEntriesResponse(false, currentTerm)
}

func (r *Candidate) processRequestVoteRequest(req RequestVoteRequest) RequestVoteResponse {
	currentTerm := r.storage.MustGetCurrentTerm()
	votedFor := r.storage.MustGetVotedFor()
	// lastLogEntry := r.storage.MustGetLastLogEntry()

	if req.Term < currentTerm {
		return newRequestVoteResponse(false, currentTerm, "")
	}

	if req.Term > currentTerm {
		r.followTerm(req.Term)
	} else if votedFor == "" || votedFor == req.CandidateID {
		return newRequestVoteResponse(false, currentTerm, "")
	}
	return newRequestVoteResponse(false, currentTerm, "")
}

// runVote broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *Candidate) runVote(grantedC chan bool) error {
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

func (r *Candidate) followTerm(term uint64) {
	currentTerm := r.storage.MustGetCurrentTerm()
	if !(term >= currentTerm) {
		panic("must term >= currentTerm")
	}
	// degrade to FOLLOWER when found someone's term greater than ours in RequestVote
	r.setState(FOLLOWER)
	r.storage.PutCurrentTerm(term)
	// reset the votedFor after term increased
	r.storage.PutVotedFor("")
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

func TimerBetween(min, max time.Duration) *time.Timer {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	delta := time.Duration(rand.Int63n(int64(max - min)))
	return time.NewTimer(min + delta)
}
