package raft

import (
	"fmt"
	"github.com/Fleurer/miniraft/pkg/storage"
)

func (r *raft) processShowStatusRequest(req ShowStatusMessage) ShowStatusReply {
	b := ShowStatusReply{}
	b.Term = r.storage.MustGetCurrentTerm()
	b.CommitIndex = r.storage.MustGetCommitIndex()
	b.Peers = r.peers
	b.State = r.state
	return b
}

func (r *raft) processAppendEntriesRequest(req AppendEntriesMessage) AppendEntriesReply {
	currentTerm := r.storage.MustGetCurrentTerm()
	lastLogIndex, _ := r.storage.MustGetLastLogIndexAndTerm()

	// r.logger.Debugf("raft.process-append-entries msg=%#v currentTerm=%d lastLogIndex=%d", msg, currentTerm, lastLogIndex)

	if req.Term < currentTerm {
		return newAppendEntriesReply(false, currentTerm, lastLogIndex, "msg.Term < currentTerm")
	}

	if req.Term == currentTerm {
		if r.state == LEADER {
			return newAppendEntriesReply(false, currentTerm, lastLogIndex, "i'm leader")
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
		return newAppendEntriesReply(false, currentTerm, lastLogIndex, "log not match")
	}

	if req.PrevLogIndex < lastLogIndex {
		r.storage.TruncateSince(req.PrevLogIndex + 1)
	}

	r.storage.AppendLogEntries(req.LogEntries)
	r.storage.PutCommitIndex(req.CommitIndex)

	lastLogIndex, _ = r.storage.MustGetLastLogIndexAndTerm()
	return newAppendEntriesReply(true, currentTerm, lastLogIndex, "success")
}

func (r *raft) processRequestVoteRequest(req RequestVoteMessage) RequestVoteReply {
	currentTerm := r.storage.MustGetCurrentTerm()
	votedFor := r.storage.MustGetVotedFor()
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()

	r.logger.Debugf("raft.process-request-vote msg=%#v currentTerm=%d votedFor=%s lastLogIndex=%d lastLogTerm=%d", req, currentTerm, votedFor, lastLogIndex, lastLogTerm)
	// if the caller's term smaller than mine, refuse
	if req.Term < currentTerm {
		return newRequestVoteReply(false, currentTerm, fmt.Sprintf("msg.term: %d < curremtTerm: %d", req.Term, currentTerm))
	}

	// if the term is equal and we've already voted for another candidate
	if req.Term == currentTerm && votedFor != "" && votedFor != req.CandidateID {
		return newRequestVoteReply(false, currentTerm, fmt.Sprintf("I've already voted another candidate: %s", votedFor))
	}

	// if the caller's term bigger than my term: set currentTerm = T, convert to follower
	if req.Term > currentTerm {
		r.setState(FOLLOWER)
		r.storage.PutCurrentTerm(req.Term)
		r.storage.PutVotedFor(req.CandidateID)
	}

	// if the candidate's log is not at least as update as our last log
	if lastLogIndex > req.LastLogIndex || lastLogTerm > req.LastLogTerm {
		return newRequestVoteReply(false, currentTerm, "candidate's log not at least as update as our last log")
	}

	r.storage.PutVotedFor(req.CandidateID)
	return newRequestVoteReply(true, currentTerm, "cheers, granted")
}

func (r *raft) processCommandRequest(req CommandMessage) CommandReply {
	switch req.Command.OpType {
	case kNop:
		return CommandReply{Value: []byte{}, Message: "nop"}
	case kPut:
		logIndex, _ := r.storage.AppendLogEntriesByCommands([]storage.RaftCommand{req.Command})
		// TODO: await logIndex got commit
		return CommandReply{Value: []byte{}, Message: fmt.Sprintf("logIndex: %d", logIndex)}
	case kGet:
		v, exists := r.storage.MustGetKV(req.Command.Key)
		if !exists {
			return CommandReply{Value: nil, Message: "not found"}
		}
		return CommandReply{Value: v, Message: "success"}
	default:
		panic(fmt.Sprintf("unexpected opType: %s", req.Command.OpType))
	}
}
