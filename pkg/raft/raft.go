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

type Raft struct {
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
		ID:        opt.ID,
		state:     FOLLOWER,
		peers:     peers,
		storage:   storage,
		requester: NewRaftRequester(),

		heartbeatInterval: 100 * time.Millisecond,
		electionTimeout:   5 * time.Second,
		clock:             clock.New(),

		reqc:   make(chan interface{}),
		respc:  make(chan interface{}),
		closed: make(chan struct{}),
	}
	r.logger = NewRaftLogger(r, DEBUG)
	return r, nil
}

func (r *Raft) Loop() {
	r.logger.Debugf("raft.loop.start: storage=%s peers=%v", r.storage.path, r.peers)
	for {
		switch r.state {
		case FOLLOWER:
			r.loopFollower()
		case LEADER:
			r.loopLeader()
		case CANDIDATE:
			r.loopCandidate()
		case CLOSED:
			r.logger.Debugf("raft.loop.closed")
			break
		}
	}
}

func (r *Raft) loopFollower() {
	electionTimer := r.newElectionTimer()
	for r.state == FOLLOWER {
		select {
		case <-electionTimer.C:
			r.logger.Debugf("follower.loop.electionTimeout")
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
func (r *Raft) loopCandidate() {
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

func (r *Raft) loopLeader() {
	nextLogIndexes := map[string]uint64{} // TODO: 初始化为当前最长 log index + 1
	heartbeatTicker := r.clock.Ticker(r.heartbeatInterval)
	for r.state == LEADER {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-heartbeatTicker.C:
			r.broadcastHeartbeats(nextLogIndexes)
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

func (r *Raft) Shutdown() {
	r.logger.Debugf("raft.shutdown")
	close(r.closed)
}

func (r *Raft) closeRaft() {
	r.state = CLOSED
	r.storage.Close()
	close(r.reqc)
	close(r.respc)
}

func (r *Raft) setState(s string) {
	r.logger.Debugf("raft.setState state=%s", s)
	r.state = s
}

func (r *Raft) newElectionTimer() *clock.Timer {
	return NewTimerBetween(r.clock, r.electionTimeout, r.electionTimeout*2)
}
