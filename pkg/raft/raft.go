package raft

import (
	"fmt"
	"time"

	"github.com/facebookgo/clock"
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

type Raft interface {
	Tick(n uint64) error
	Loop()
	ProcessRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error)
	ProcessAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	ProcessCommand(req *CommandRequest) (*CommandResponse, error)
	Shutdown()
}

type raft struct {
	ID    string
	state string
	peers map[string]Peer

	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	clock             clock.Clock

	logger    *RaftLogger
	storage   *RaftStorage
	requester RaftRequester

	reqc   chan interface{}
	respc  chan interface{}
	closed chan struct{}
}

type RaftOptions struct {
	ID           string            `json:"id"`
	StoragePath  string            `json:"storagePath"`
	ListenAddr   string            `json:"listenAddr"`
	PeerAddr     string            `json:"peerAddr"`
	InitialPeers map[string]string `json:"initialPeers"`
}

func NewRaft(opt *RaftOptions) (Raft, error) {
	return newRaft(opt)
}

func newRaft(opt *RaftOptions) (*raft, error) {
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

	r := &raft{}
	r.ID = opt.ID
	r.state = FOLLOWER
	r.heartbeatInterval = 100 * time.Millisecond
	r.electionTimeout = 5 * time.Second
	r.peers = peers
	r.storage = storage
	r.clock = clock.New()
	r.logger = NewRaftLogger(r, DEBUG)
	r.requester = NewRaftRequester(r.logger)
	r.reqc = make(chan interface{})
	r.respc = make(chan interface{})
	r.closed = make(chan struct{})
	return r, nil
}

func (r *raft) ProcessAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	r.reqc <- req
	resp := <-r.respc
	return resp.(*AppendEntriesResponse), nil
}

func (r *raft) ProcessCommand(req *CommandRequest) (*CommandResponse, error) {
	r.reqc <- req
	resp := <-r.respc
	return resp.(*CommandResponse), nil
}

func (r *raft) ProcessRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	r.reqc <- req
	resp := <-r.respc
	return resp.(*RequestVoteResponse), nil
}

func (r *raft) Tick(n uint64) error {
	return nil
}

func (r *raft) Loop() {
	r.logger.Infof("raft.loop.start: storage=%s peers=%v", r.storage.path, r.peers)
	for {
		switch r.state {
		case FOLLOWER:
			r.loopFollower()
		case LEADER:
			r.loopLeader()
		case CANDIDATE:
			r.loopCandidate()
		case CLOSED:
			r.logger.Infof("raft.loop.closed")
			break
		}
	}
}

func (r *raft) loopFollower() {
	electionTimer := r.newElectionTimer()
	for r.state == FOLLOWER {
		select {
		case <-electionTimer.C:
			r.logger.Infof("follower.loop.electionTimeout")
			r.setState(CANDIDATE)
		case <-r.closed:
			r.closeRaft()
		case ev := <-r.reqc:
			switch req := ev.(type) {
			case AppendEntriesRequest:
				r.respc <- r.processAppendEntriesRequest(req)
				electionTimer = r.newElectionTimer()
			case RequestVoteRequest:
				r.respc <- r.processRequestVoteRequest(req)
			case ShowStatusRequest:
				r.respc <- r.processShowStatusRequest(req)
			default:
				r.respc <- newServerResponse(400, fmt.Sprintf("invalid request for follower: %v", req))
			}
		}
	}
}

// After a candidate raise a rote:
// 它自己赢得选举；
// 另一台机器宣称自己赢得选举；
// 一段时间过后没有赢家
func (r *raft) loopCandidate() {
	grantedC := make(chan bool)
	electionTimer := r.newElectionTimer()
	r.runElection(grantedC)
	for r.state == CANDIDATE {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-electionTimer.C:
			r.runElection(grantedC)
			electionTimer = r.newElectionTimer()
		case granted := <-grantedC:
			if granted {
				r.setState(LEADER)
				continue
			} else {

			}
		case ev := <-r.reqc:
			switch req := ev.(type) {
			case RequestVoteRequest:
				r.respc <- r.processRequestVoteRequest(req)
			case ShowStatusRequest:
				r.respc <- r.processShowStatusRequest(req)
			default:
				r.respc <- newServerResponse(400, fmt.Sprintf("invalid request for candidate: %v", req))
			}
		}
	}
}

func (r *raft) loopLeader() {
	l := NewRaftLeader(r)
	heartbeatTicker := r.clock.Ticker(r.heartbeatInterval)
	for r.state == LEADER {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-heartbeatTicker.C:
			l.broadcastHeartbeats()
		case ev := <-r.reqc:
			switch req := ev.(type) {
			case AppendEntriesRequest:
				r.respc <- r.processAppendEntriesRequest(req)
			case RequestVoteRequest:
				r.respc <- r.processRequestVoteRequest(req)
			case ShowStatusRequest:
				r.respc <- r.processShowStatusRequest(req)
			case CommandRequest:
				r.respc <- r.processCommandRequest(req)
			default:
				r.respc <- newServerResponse(400, fmt.Sprintf("invalid request for leader: %v", req))
			}
		}
	}
}

func (r *raft) Shutdown() {
	r.logger.Debugf("raft.shutdown")
	close(r.closed)
}

func (r *raft) closeRaft() {
	r.logger.Infof("raft.close-raft")
	r.state = CLOSED
	r.storage.Close()
	close(r.reqc)
	close(r.respc)
}

func (r *raft) setState(s string) {
	r.logger.Debugf("raft.set-state state=%s", s)
	r.state = s
}

func (r *raft) newElectionTimer() *clock.Timer {
	return NewTimerBetween(r.clock, r.electionTimeout, r.electionTimeout*2)
}
