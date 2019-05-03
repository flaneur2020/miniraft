package raft

const (
	SUCCESS        = 200
	BAD_REQUEST    = 400
	INTERNAL_ERROR = 500
)

type AppendEntriesRequest struct {
	Term         uint64 `json:"term"`
	LeaderPeer   Peer   `json:"leaderPeer"`
	CommitIndex  uint64 `json:"commitIndex"`
	PrevLogIndex uint64 `json:"prevLogIndex"`
	PrevLogTerm  uint64 `json:"prevLogTerm"`

	LogEntries []RaftLogEntry `json:"logEntries,omitempty"`
}

type AppendEntriesResponseBody struct {
}

type RequestVoteRequest struct {
	Term          uint64 `json:"term"`
	CandidatePeer Peer   `json:"candidatePeer"`
	LastLogIndex  uint64 `json:"lastLogIndex"`
	LastLogTerm   uint64 `json:"lasstLogTerm"`
}

type RequestVoteResponseBody struct {
}

type RaftLogEntry struct {
	Term uint64
}

type RaftResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Body    interface{} `json:"body,omitempty"`
}
