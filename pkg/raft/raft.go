package raft

import (
	"fmt"
	"github.com/Fleurer/miniraft/pkg/storage"
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
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type Raft interface {
	Tick(n uint64) error
	Loop()
	Process(req interface{}) (interface{}, error)
	Shutdown()
}

type raft struct {
	ID    string
	state string
	peers map[string]Peer

	nextLogIndexes map[string]uint64

	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	clock             clock.Clock

	logger    *Logger
	storage   storage.RaftStorage
	requester RaftRequester

	eventc chan raftEV
	closed chan struct{}
}

type RaftOptions struct {
	ID           string            `json:"id"`
	StoragePath  string            `json:"storagePath"`
	ListenAddr   string            `json:"listenAddr"`
	PeerAddr     string            `json:"peerAddr"`
	InitialPeers map[string]string `json:"initialPeers"`
}

type raftEV struct {
	req interface{}
	respc chan interface{}
}

func newRaftEV(req interface{}) raftEV {
	return raftEV{req, make(chan interface{}, 1)}
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
	s, err := storage.NewRaftStorage(opt.StoragePath, prefix)
	if err != nil {
		return nil, err
	}

	r := &raft{}
	r.ID = opt.ID
	r.state = FOLLOWER
	r.heartbeatInterval = 100 * time.Millisecond
	r.electionTimeout = 5 * time.Second
	r.peers = peers
	r.storage = s
	r.nextLogIndexes = map[string]uint64{}
	r.clock = clock.New()
	r.logger = NewRaftLogger(r.ID, DEBUG)
	r.requester = NewRaftRequester(r.logger)
	r.eventc = make(chan raftEV)
	r.closed = make(chan struct{})
	return r, nil
}

func (r *raft) Process(req interface{}) (interface{}, error) {
	ev := newRaftEV(req)
	r.eventc <- ev
	resp := <- ev.respc
	close(ev.respc)
	return resp, nil
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
		case ev := <-r.eventc:
			switch req := ev.req.(type) {
			case data.AppendEntriesRequest:
				ev.respc <- r.processAppendEntriesRequest(req)
				electionTimer = r.newElectionTimer()
			case data.RequestVoteRequest:
				ev.respc <- r.processRequestVoteRequest(req)
			case data.ShowStatusRequest:
				ev.respc <- r.processShowStatusRequest(req)
			default:
				ev.respc <- data.newServerResponse(400, fmt.Sprintf("invalid request for follower: %v", req))
			}
		}
	}
}

// After a candidate raise a rote:
// 它自己赢得选举；
// 另一台机器宣称自己赢得选举；
// 一段时间过后没有赢家
func (r *raft) loopCandidate() {
	electionResultC := make(chan bool)
	electionTimer := r.newElectionTimer()
	go func() {
		electionResultC <- r.runElection()
	}()

	for r.state == CANDIDATE {
		select {
		case <-r.closed:
			r.closeRaft()

		case <-electionTimer.C:
			go func() {
				electionResultC <- r.runElection()
			}()
			electionTimer = r.newElectionTimer()

		case ok := <-electionResultC:
			if ok {
				r.setState(LEADER)
				continue
			}

		case ev := <-r.eventc:
			switch req := ev.req.(type) {
			case data.RequestVoteRequest:
				ev.respc <- r.processRequestVoteRequest(req)
			case data.ShowStatusRequest:
				ev.respc <- r.processShowStatusRequest(req)
			default:
				ev.respc <- data.newServerResponse(400, fmt.Sprintf("invalid request for candidate: %v", req))
			}
		}
	}
}

func (r *raft) loopLeader() {
	r.resetLeader()
	heartbeatTicker := r.clock.Ticker(r.heartbeatInterval)
	for r.state == LEADER {
		select {
		case <-r.closed:
			r.closeRaft()
		case <-heartbeatTicker.C:
			r.broadcastHeartbeats()
		case ev := <-r.eventc:
			switch req := ev.req.(type) {
			case data.AppendEntriesRequest:
				ev.respc <- r.processAppendEntriesRequest(req)
			case data.RequestVoteRequest:
				ev.respc <- r.processRequestVoteRequest(req)
			case data.ShowStatusRequest:
				ev.respc <- r.processShowStatusRequest(req)
			case data.CommandRequest:
				ev.respc <- r.processCommandRequest(req)
			default:
				ev.respc <- data.newServerResponse(400, fmt.Sprintf("invalid request for leader: %v", req))
			}
		}
	}
}

func (r *raft) resetLeader() {
	lastLogIndex, _ := r.storage.MustGetLastLogIndexAndTerm()
	for _, p := range r.peers {
		r.nextLogIndexes[p.ID] = lastLogIndex
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
	close(r.eventc)
}

func (r *raft) setState(s string) {
	r.logger.Debugf("raft.set-state state=%s", s)
	r.state = s
}

func (r *raft) newElectionTimer() *clock.Timer {
	return NewTimerBetween(r.clock, r.electionTimeout, r.electionTimeout*2)
}
