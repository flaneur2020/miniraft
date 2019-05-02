package raft

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
	peers map[string]*Peer

	F *Follower
	L *Leader
	C *Candidate

	s *RaftStorage

	requestChan  chan interface{}
	responseChan chan interface{}
	closed       chan struct{}
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
