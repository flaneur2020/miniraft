package raft

import (
	"sync"
)

const (
	FOLLOWER  = "follower"
	LEADER    = "leader"
	CANDIDATE = "candidate"
)

type Peer struct {
	addr string
}

type Raft struct {
	ID          string
	Epoch       int64
	LogIndex    int
	CommitIndex int
	state       string
	mutex       sync.Mutex
	peers       map[string]*Peer

	F *Follower
	L *Leader
	C *Candidate
}

type RaftOptions struct {
	clientAddr       string
	peerAddr         string
	initialPeerAddrs []string
}

func NewRaft(opt *RaftOptions) *Raft {
	return nil
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
