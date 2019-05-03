package raft

type AppendEntriesRequest struct {
	Term        uint64
	CommitIndex uint64
	LeaderID    string
}

type AppendEntriesResponse struct {
}

type RequestVoteRequest struct {
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
}
