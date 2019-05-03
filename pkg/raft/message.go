package raft

const (
	SUCCESS        = 200
	NOT_FOUND      = 404
	BAD_REQUEST    = 400
	INTERNAL_ERROR = 500
)

const (
	OP_WRTIE  = 1
	OP_DELETE = 2
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
	Term         uint64 `json:"term"`
	Success      bool   `json:"success"`
	LastLogIndex uint64 `json:"lastLogIndex"`
}

type RequestVoteRequest struct {
	Term          uint64 `json:"term"`
	CandidatePeer Peer   `json:"candidatePeer"`
	LastLogIndex  uint64 `json:"lastLogIndex"`
	LastLogTerm   uint64 `json:"lasstLogTerm"`
}

type RequestVoteResponseBody struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

type ShowStatusRequest struct {
}

type ShowStatusResponseBody struct {
	Term        uint64          `json:"term"`
	CommitIndex uint64          `json:"commitIndex"`
	Peers       map[string]Peer `json:"peers"`
	State       string          `json:"state"`
}

type RaftLogEntry struct {
	OpType int    `json:"opType"`
	Term   uint64 `json:"term"`
	Key    []byte `json:"key"`
	Value  []byte `json:"value"`
}

type RaftResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Body    interface{} `json:"body,omitempty"`
}
