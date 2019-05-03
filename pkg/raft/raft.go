package raft

import "fmt"

const (
	FOLLOWER  = "follower"
	LEADER    = "leader"
	CANDIDATE = "candidate"
)

type Peer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type Raft struct {
	ID    string
	state string
	peers map[string]Peer

	F *Follower
	L *Leader
	C *Candidate

	storage *RaftStorage

	requestChan  chan interface{}
	responseChan chan interface{}
}

type RaftOptions struct {
	ID           string
	StoragePath  string
	ListenAddr   string
	PeerAddr     string
	InitialPeers map[string]string
}

func NewRaft(opt *RaftOptions) (*Raft, error) {
	peers := map[string]Peer{}
	for id, addr := range opt.InitialPeers {
		peers[id] = Peer{ID: id, Addr: addr}
	}

	prefix := fmt.Sprintf("rft:%s:", opt.ID)
	storage, err := NewRaftStorage(opt.StoragePath, prefix)
	if err != nil {
		return nil, err
	}

	r := &Raft{
		ID:           opt.ID,
		state:        FOLLOWER,
		peers:        peers,
		storage:      storage,
		requestChan:  make(chan interface{}),
		responseChan: make(chan interface{}),
	}
	r.F = NewFollower(r)
	r.L = NewLeader(r)
	r.C = NewCandidate(r)
	return r, nil
}

func (r *Raft) Loop() {
	switch r.state {
	case FOLLOWER:
		r.F.Loop()
	case LEADER:
		r.L.Loop()
	case CANDIDATE:
		r.C.Loop()
	}
}

func (r *Raft) Close() {
	r.storage.Close()
	close(r.requestChan)
	close(r.responseChan)
}
