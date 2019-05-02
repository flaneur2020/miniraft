package raft

type AppendEntriesRequest struct {
	Term int64
}

type RequestVoteRequest struct {
	Term int64
}
