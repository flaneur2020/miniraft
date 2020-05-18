package raft

import (
	"github.com/Fleurer/miniraft/pkg/storage"
)

const (
	SUCCESS        = 200
	NOT_FOUND      = 404
	BAD_REQUEST    = 400
	INTERNAL_ERROR = 500
)

type RaftMessage interface {
	MessageKind() string
}

type RaftReply interface {
	ReplyKind() string
}

type ElectionTimeoutMsg struct{}

func (m *ElectionTimeoutMsg) MessageKind() string {
	return "election-timeout"
}

type ElectionResultMsg struct {
	granted bool
}

func (m *ElectionResultMsg) MessageKind() string {
	return "election-result"
}

type HeartbeatTimeoutMsg struct{}

func (m *HeartbeatTimeoutMsg) MessageKind() string {
	return "heartbeat-timeout"
}

type AppendEntriesMsg struct {
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leaderID"`
	CommitIndex  uint64 `json:"commitIndex"`
	PrevLogIndex uint64 `json:"prevLogIndex"`
	PrevLogTerm  uint64 `json:"prevLogTerm"`

	LogEntries []storage.RaftLogEntry `json:"logEntries,omitempty"`
}

func (m *AppendEntriesMsg) MessageKind() string {
	return "append-entries"
}

type AppendEntriesReply struct {
	PeerID       string `json:"peerID"`
	Term         uint64 `json:"term"`
	Success      bool   `json:"success"`
	Message      string `json:"message"`
	LastLogIndex uint64 `json:"last_log_index"`
}

func (m *AppendEntriesReply) ReplyKind() string {
	return "append-entries"
}

type RequestVoteMsg struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateID"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lasstLogTerm"`
}

func (m *RequestVoteMsg) MessageKind() string {
	return "request-vote"
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
	Message     string `json:"message"`
}

func (r *RequestVoteReply) ReplyKind() string {
	return "request-vote"
}

type ShowStatusMsg struct {
}

func (m *ShowStatusMsg) MessageKind() string {
	return "show-status"
}

type ShowStatusReply struct {
	Term        uint64          `json:"term"`
	CommitIndex uint64          `json:"commitIndex"`
	Peers       map[string]Peer `json:"peers"`
	State       string          `json:"state"`
}

func (r *ShowStatusReply) ReplyKind() string {
	return "show-status"
}

type CommandMessage struct {
	Command storage.RaftCommand `json:"command"`
}

func (m *CommandMessage) MessageKind() string {
	return "command"
}

type CommandReply struct {
	Message string `json:"message"`
	Value   []byte `json:"value,omitempty"`
}

func (r *CommandReply) ReplyKind() string {
	return "command"
}

type MessageReply struct {
	Code    int    `code:"code"`
	Message string `json:"message"`
}

func (r *MessageReply) ReplyKind() string {
	return "message"
}

func newRequestVoteReply(success bool, term uint64, message string) *RequestVoteReply {
	return &RequestVoteReply{VoteGranted: success, Term: term, Message: message}
}

func newAppendEntriesReply(success bool, term uint64, lastLogIndex uint64, peerID string, message string) *AppendEntriesReply {
	return &AppendEntriesReply{Success: success, Term: term, LastLogIndex: lastLogIndex, PeerID: peerID, Message: message}
}

func newMessageReply(code int, message string) *MessageReply {
	return &MessageReply{Code: code, Message: message}
}
