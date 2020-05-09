package raft

import (
	"github.com/Fleurer/miniraft/pkg/storage"
)

func (r *raft) broadcastHeartbeats() error {
	messages, err := r.buildAppendEntriesMessages(r.nextLogIndexes)
	if err != nil {
		return err
	}

	r.logger.Debugf("leader.broadcast-heartbeats messages=%v", messages)
	for id, msg := range messages {
		p := r.peers[id]
		_, err := r.requester.SendAppendEntries(p, msg)

		// TODO: 增加回退 nextLogIndex 逻辑
		if err != nil {
			return err
		}
	}
	return nil
}

// runElection broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *raft) runElection() bool {
	if r.state != CANDIDATE {
		panic("should be candidate")
	}

	// increase candidate's term and vote for itself
	currentTerm := r.storage.MustGetCurrentTerm()
	r.storage.PutCurrentTerm(currentTerm + 1)
	r.storage.PutVotedFor(r.ID)
	r.logger.Debugf("raft.candidate.vote term=%d votedFor=%s", currentTerm, r.ID)

	// send requestVote messages asynchronously, collect the vote results into grantedC
	messages, err := r.buildRequestVoteMessages()
	if err != nil {
		r.logger.Debugf("raft.candidate.vote.buildRequestVoteMessages err=%s", err)
		return false
	}

	peers := map[string]Peer{}
	for id, p := range r.peers {
		peers[id] = p
	}

	granted := 0
	for id, msg := range messages {
		p := peers[id]
		resp, err := r.requester.SendRequestVote(p, msg)
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

func (r *raft) buildRequestVoteMessages() (map[string]*RequestVoteMessage, error) {
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()
	currentTerm := r.storage.MustGetCurrentTerm()

	messages := map[string]*RequestVoteMessage{}
	for id := range r.peers {
		msg := RequestVoteMessage{}
		msg.CandidateID = r.ID
		msg.LastLogIndex = lastLogIndex
		msg.LastLogTerm = lastLogTerm
		msg.Term = currentTerm
		messages[id] = &msg
	}
	return messages, nil
}

func (r *raft) buildAppendEntriesMessages(nextLogIndexes map[string]uint64) (map[string]*AppendEntriesMessage, error) {
	messages := map[string]*AppendEntriesMessage{}
	for id, idx := range nextLogIndexes {
		msg := &AppendEntriesMessage{}
		msg.LeaderID = r.ID
		msg.LogEntries = []storage.RaftLogEntry{}
		msg.Term = r.storage.MustGetCurrentTerm()
		msg.CommitIndex = r.storage.MustGetCommitIndex()

		if idx == 0 {
			msg.PrevLogIndex = 0
			msg.PrevLogTerm = 0
		} else {
			logEntries := r.storage.MustGetLogEntriesSince(idx - 1)
			if len(logEntries) >= 1 {
				msg.PrevLogIndex = logEntries[0].Index
				msg.PrevLogTerm = logEntries[0].Term
			}
			if len(logEntries) >= 2 {
				msg.LogEntries = logEntries[1:]
			}
		}
		messages[id] = msg
	}
	return messages, nil
}
