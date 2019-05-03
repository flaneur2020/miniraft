package raft

import (
	"fmt"
	"log"
	"time"
)

const (
	FOLLOWER  = "follower"
	LEADER    = "leader"
	CANDIDATE = "candidate"
	CLOSED    = "closed"
)

type Peer struct {
	ID   string
	Addr string
}

type Raft struct {
	ID    string
	state string
	peers map[string]Peer

	electionTimeout time.Duration

	F *Follower
	L *Leader
	C *Candidate

	storage *RaftStorage

	reqc   chan interface{}
	respc  chan RaftResponse
	closed chan struct{}
}

type RaftOptions struct {
	ID           string            `json:"id"`
	StoragePath  string            `json:"storagePath"`
	ListenAddr   string            `json:"listenAddr"`
	PeerAddr     string            `json:"peerAddr"`
	InitialPeers map[string]string `json:"initialPeers"`
}

func NewRaft(opt *RaftOptions) (*Raft, error) {
	peers := map[string]Peer{}
	for id, addr := range opt.InitialPeers {
		if id == opt.ID {
			continue
		}
		peers[id] = Peer{ID: id, Addr: addr}
	}

	prefix := fmt.Sprintf("rft:%s:", opt.ID)
	storage, err := NewRaftStorage(opt.StoragePath, prefix)
	if err != nil {
		return nil, err
	}

	r := &Raft{
		ID:      opt.ID,
		state:   FOLLOWER,
		peers:   peers,
		storage: storage,

		electionTimeout: 20 * time.Second,

		reqc:   make(chan interface{}),
		respc:  make(chan RaftResponse),
		closed: make(chan struct{}),
	}
	r.F = NewFollower(r)
	r.L = NewLeader(r)
	r.C = NewCandidate(r)
	return r, nil
}

func (r *Raft) Loop() {
	log.Printf("raft.loop.start: id=%s state=%s storage=%s peers=%v", r.ID, r.state, r.storage.path, r.peers)
	for {
		switch r.state {
		case FOLLOWER:
			r.F.Loop()
		case LEADER:
			r.L.Loop()
		case CANDIDATE:
			r.C.Loop()
		case CLOSED:
			log.Printf("raft.loop.closed id=%s", r.ID)
			break
		}
	}
}

func (r *Raft) Shutdown() {
	log.Printf("raft.shutdown id=%s", r.ID)
	close(r.closed)
}

func (r *Raft) closeRaft() {
	r.state = CLOSED
	r.storage.Close()
	close(r.reqc)
	close(r.respc)
}

func (r *Raft) setState(s string) {
	r.state = s
}

func (r *Raft) sendAppendEntriesRequest(req *AppendEntriesRequest) *RaftResponse {
	return &RaftResponse{}
}

func (r *Raft) sendRequestVoteRequest(req *RequestVoteRequest) *RaftResponse {
	return &RaftResponse{}
}

func (r *Raft) processShowStatusRequest(req ShowStatusRequest) RaftResponse {
	b := ShowStatusResponseBody{}
	b.Term, _ = r.storage.GetCurrentTerm()
	b.CommitIndex, _ = r.storage.GetCommitIndex()
	b.Peers = r.peers
	b.State = r.state
	return RaftResponse{Code: SUCCESS, Message: "success", Body: b}
}
