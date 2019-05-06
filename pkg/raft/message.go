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
	LeaderID     string `json:"leaderID"`
	CommitIndex  uint64 `json:"commitIndex"`
	PrevLogIndex uint64 `json:"prevLogIndex"`
	PrevLogTerm  uint64 `json:"prevLogTerm"`

	LogEntries []RaftLogEntry `json:"logEntries,omitempty"`
}

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateID"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lasstLogTerm"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
	Message     string `json:"message"`
}

type ShowStatusRequest struct {
}

type ShowStatusResponse struct {
	Term        uint64          `json:"term"`
	CommitIndex uint64          `json:"commitIndex"`
	Peers       map[string]Peer `json:"peers"`
	State       string          `json:"state"`
}

type ServerResponse struct {
	Code    int    `code:"code"`
	Message string `json:"message"`
}

type RaftLogEntry struct {
	OpType int    `json:"opType"`
	Term   uint64 `json:"term"`
	Index  uint64 `json:"index"`
}

func newRequestVoteResponse(success bool, term uint64, message string) RequestVoteResponse {
	return RequestVoteResponse{VoteGranted: success, Term: term, Message: message}
}

func newAppendEntriesResponse(success bool, term uint64) AppendEntriesResponse {
	return AppendEntriesResponse{Success: success, Term: term}
}

func newServerResponse(code int, message string) ServerResponse {
	return ServerResponse{Code: code, Message: message}
}
