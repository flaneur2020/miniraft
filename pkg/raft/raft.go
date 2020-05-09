package raft

import (
	"fmt"
	"time"

	"github.com/Fleurer/miniraft/pkg/storage"
	"github.com/Fleurer/miniraft/pkg/util"
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
	Process(msg interface{}) (interface{}, error)
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

	logger    *util.Logger
	storage   storage.RaftStorage
	requester RaftSender

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
	msg    interface{}
	replyc chan interface{}
}

func newRaftEV(msg interface{}) raftEV {
	return raftEV{msg, make(chan interface{}, 1)}
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
	r.logger = util.NewRaftLogger(r.ID, util.DEBUG)
	r.requester = NewRaftSender(r.logger)
	r.eventc = make(chan raftEV)
	r.closed = make(chan struct{})
	return r, nil
}

func (r *raft) Process(msg interface{}) (interface{}, error) {
	ev := newRaftEV(msg)
	r.eventc <- ev
	reply := <-ev.replyc
	close(ev.replyc)
	return reply, nil
}

func (r *raft) Tick(n uint64) error {
	return nil
}

func (r *raft) Loop() {
	r.logger.Infof("raft.loop.start: peers=%v", r.peers)
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
			r.become(CANDIDATE)
		case <-r.closed:
			r.closeRaft()
		case ev := <-r.eventc:
			switch msg := ev.msg.(type) {
			case *AppendEntriesMessage:
				ev.replyc <- r.processAppendEntries(msg)
				electionTimer = r.newElectionTimer()
			case *RequestVoteMessage:
				ev.replyc <- r.processRequestVote(msg)
			case *ShowStatusMessage:
				ev.replyc <- r.processShowStatus(msg)
			default:
				ev.replyc <- newServerReply(400, fmt.Sprintf("invalid request %T for follower: %v", ev.msg, ev.msg))
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
				r.become(LEADER)
				continue
			}

		case ev := <-r.eventc:
			switch msg := ev.msg.(type) {
			case *RequestVoteMessage:
				ev.replyc <- r.processRequestVote(msg)
			case *ShowStatusMessage:
				ev.replyc <- r.processShowStatus(msg)
			default:
				ev.replyc <- newServerReply(400, fmt.Sprintf("invalid msg for candidate: %T", msg))
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
			switch msg := ev.msg.(type) {
			case *AppendEntriesMessage:
				ev.replyc <- r.processAppendEntries(msg)
			case *RequestVoteMessage:
				ev.replyc <- r.processRequestVote(msg)
			case *ShowStatusMessage:
				ev.replyc <- r.processShowStatus(msg)
			case *CommandMessage:
				ev.replyc <- r.processCommand(msg)
			default:
				ev.replyc <- newServerReply(400, fmt.Sprintf("invalid msg for leader: %T", msg))
			}
		}
	}
}

func (r *raft) processShowStatus(msg *ShowStatusMessage) *ShowStatusReply {
	b := ShowStatusReply{}
	b.Term = r.storage.MustGetCurrentTerm()
	b.CommitIndex = r.storage.MustGetCommitIndex()
	b.Peers = r.peers
	b.State = r.state
	return &b
}

func (r *raft) processAppendEntries(msg *AppendEntriesMessage) *AppendEntriesReply {
	currentTerm := r.storage.MustGetCurrentTerm()
	lastLogIndex, _ := r.storage.MustGetLastLogIndexAndTerm()

	// r.logger.Debugf("raft.process-append-entries msg=%#v currentTerm=%d lastLogIndex=%d", msg, currentTerm, lastLogIndex)

	if msg.Term < currentTerm {
		return newAppendEntriesReply(false, currentTerm, lastLogIndex, "msg.Term < currentTerm")
	}

	if msg.Term == currentTerm {
		if r.state == LEADER {
			return newAppendEntriesReply(false, currentTerm, lastLogIndex, "i'm leader")
		}
		if r.state == CANDIDATE {
			// while waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader
			// if the leader's term is at least as large as the candidate's current term, then the candidate recognizes the leader
			// as legitimate and returns to follower state.
			r.become(FOLLOWER)
		}
	}

	if msg.Term > currentTerm {
		r.become(FOLLOWER)
		r.storage.PutCurrentTerm(msg.Term)
		r.storage.PutVotedFor("")
	}

	if msg.PrevLogIndex > lastLogIndex {
		return newAppendEntriesReply(false, currentTerm, lastLogIndex, "log not match")
	}

	if msg.PrevLogIndex < lastLogIndex {
		r.storage.TruncateSince(msg.PrevLogIndex + 1)
	}

	r.storage.AppendLogEntries(msg.LogEntries)
	r.storage.PutCommitIndex(msg.CommitIndex)

	lastLogIndex, _ = r.storage.MustGetLastLogIndexAndTerm()
	return newAppendEntriesReply(true, currentTerm, lastLogIndex, "success")
}

func (r *raft) processRequestVote(msg *RequestVoteMessage) *RequestVoteReply {
	currentTerm := r.storage.MustGetCurrentTerm()
	votedFor := r.storage.MustGetVotedFor()
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()

	r.logger.Debugf("raft.process-request-vote msg=%#v currentTerm=%d votedFor=%s lastLogIndex=%d lastLogTerm=%d", msg, currentTerm, votedFor, lastLogIndex, lastLogTerm)
	// if the caller's term smaller than mine, refuse
	if msg.Term < currentTerm {
		return newRequestVoteReply(false, currentTerm, fmt.Sprintf("msg.term: %d < curremtTerm: %d", msg.Term, currentTerm))
	}

	// if the term is equal and we've already voted for another candidate
	if msg.Term == currentTerm && votedFor != "" && votedFor != msg.CandidateID {
		return newRequestVoteReply(false, currentTerm, fmt.Sprintf("I've already voted another candidate: %s", votedFor))
	}

	// if the caller's term bigger than my term: set currentTerm = T, convert to follower
	if msg.Term > currentTerm {
		r.become(FOLLOWER)
		r.storage.PutCurrentTerm(msg.Term)
		r.storage.PutVotedFor(msg.CandidateID)
	}

	// if the candidate's log is not at least as update as our last log
	if lastLogIndex > msg.LastLogIndex || lastLogTerm > msg.LastLogTerm {
		return newRequestVoteReply(false, currentTerm, "candidate's log not at least as update as our last log")
	}

	r.storage.PutVotedFor(msg.CandidateID)
	return newRequestVoteReply(true, currentTerm, "cheers, granted")
}

func (r *raft) processCommand(req *CommandMessage) *CommandReply {
	switch req.Command.OpType {
	case kNop:
		return &CommandReply{Value: []byte{}, Message: "nop"}
	case kPut:
		logIndex, _ := r.storage.AppendLogEntriesByCommands([]storage.RaftCommand{req.Command})
		// TODO: await logIndex got commit
		return &CommandReply{Value: []byte{}, Message: fmt.Sprintf("logIndex: %d", logIndex)}
	case kGet:
		v, exists := r.storage.MustGetKV(req.Command.Key)
		if !exists {
			return &CommandReply{Value: nil, Message: "not found"}
		}
		return &CommandReply{Value: v, Message: "success"}
	default:
		panic(fmt.Sprintf("unexpected opType: %s", req.Command.OpType))
	}
}

func (r *raft) broadcastHeartbeats() error {
	messages, err := r.buildAppendEntriesMessages(r.nextLogIndexes)
	if err != nil {
		return err
	}

	r.logger.Debugf("leader.broadcast-heartbeats messages=%v", messages)
	for id, msg := range messages {
		p := r.peers[id]
		_, err := r.requester.SendAppendEntries(p, msg)

		// TODO: 增加回退 nextLogIndex 逻辑
		if err != nil {
			return err
		}
	}
	return nil
}

// runElection broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *raft) runElection() bool {
	if r.state != CANDIDATE {
		panic("should be candidate")
	}

	// increase candidate's term and vote for itself
	currentTerm := r.storage.MustGetCurrentTerm()
	r.storage.PutCurrentTerm(currentTerm + 1)
	r.storage.PutVotedFor(r.ID)
	r.logger.Debugf("raft.candidate.vote term=%d votedFor=%s", currentTerm, r.ID)

	// send requestVote messages asynchronously, collect the vote results into grantedC
	messages, err := r.buildRequestVoteMessages()
	if err != nil {
		r.logger.Debugf("raft.candidate.vote.buildRequestVoteMessages err=%s", err)
		return false
	}

	peers := map[string]Peer{}
	for id, p := range r.peers {
		peers[id] = p
	}

	granted := 0
	for id, msg := range messages {
		p := peers[id]
		resp, err := r.requester.SendRequestVote(p, msg)
		r.logger.Debugf("raft.candidate.send-request-vote target=%s resp=%#v err=%s", id, resp, err)
		if err != nil {
			continue
		}
		if resp.VoteGranted {
			granted++
		}
	}

	success := (granted+1)*2 > len(peers)+1
	r.logger.Debugf("raft.candidate.broadcast-request-vote granted=%d total=%d success=%d", granted+1, len(r.peers)+1, success)
	return success
}

func (r *raft) buildRequestVoteMessages() (map[string]*RequestVoteMessage, error) {
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()
	currentTerm := r.storage.MustGetCurrentTerm()

	messages := map[string]*RequestVoteMessage{}
	for id := range r.peers {
		msg := RequestVoteMessage{}
		msg.CandidateID = r.ID
		msg.LastLogIndex = lastLogIndex
		msg.LastLogTerm = lastLogTerm
		msg.Term = currentTerm
		messages[id] = &msg
	}
	return messages, nil
}

func (r *raft) buildAppendEntriesMessages(nextLogIndexes map[string]uint64) (map[string]*AppendEntriesMessage, error) {
	messages := map[string]*AppendEntriesMessage{}
	for id, idx := range nextLogIndexes {
		msg := &AppendEntriesMessage{}
		msg.LeaderID = r.ID
		msg.LogEntries = []storage.RaftLogEntry{}
		msg.Term = r.storage.MustGetCurrentTerm()
		msg.CommitIndex = r.storage.MustGetCommitIndex()

		if idx == 0 {
			msg.PrevLogIndex = 0
			msg.PrevLogTerm = 0
		} else {
			logEntries := r.storage.MustGetLogEntriesSince(idx - 1)
			if len(logEntries) >= 1 {
				msg.PrevLogIndex = logEntries[0].Index
				msg.PrevLogTerm = logEntries[0].Term
			}
			if len(logEntries) >= 2 {
				msg.LogEntries = logEntries[1:]
			}
		}
		messages[id] = msg
	}
	return messages, nil
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

func (r *raft) become(s string) {
	r.logger.Debugf("raft.set-state state=%s", s)
	r.state = s
}

func (r *raft) newElectionTimer() *clock.Timer {
	return util.NewTimerBetween(r.clock, r.electionTimeout, r.electionTimeout*2)
}
