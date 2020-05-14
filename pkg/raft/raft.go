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

type RaftNode interface {
	Loop()
	Process(msg RaftMessage) (RaftReply, error)
	Shutdown()
}

type raftNode struct {
	ID    string
	state string
	peers map[string]Peer

	// volatile state on leaders
	nextIndex map[string]uint64
	matchIndex map[string]uint64

	// volatile state on all servers
	commitIndex uint64

	// persistent state on all servers
	currentTerm uint64
	votedFor    string
	lastApplied uint64

	// clock
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
	clock             clock.Clock

	logger  *util.Logger
	storage storage.RaftStorage
	rpc     RaftRPC

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
	msg    RaftMessage
	replyc chan RaftReply
}

func newRaftEV(msg RaftMessage) raftEV {
	return raftEV{msg, make(chan RaftReply, 1)}
}

func NewRaft(opt *RaftOptions) (RaftNode, error) {
	return newRaft(opt)
}

func newRaft(opt *RaftOptions) (*raftNode, error) {
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

	r := &raftNode{}
	r.ID = opt.ID
	r.state = FOLLOWER
	r.heartbeatInterval = 100 * time.Millisecond
	r.electionTimeout = 5 * time.Second
	r.peers = peers
	r.storage = s
	r.votedFor = ""
	r.nextIndex = map[string]uint64{}
	r.matchIndex = map[string]uint64{}
	r.clock = clock.New()
	r.logger = util.NewRaftLogger(r.ID, util.DEBUG)
	r.rpc = NewRaftRPC(r.logger)
	r.eventc = make(chan raftEV)
	r.closed = make(chan struct{})
	r.mustLoadState()
	return r, nil
}

func (r *raftNode) Process(msg RaftMessage) (RaftReply, error) {
	ev := newRaftEV(msg)

	select {
	case <-r.closed:
		return nil, fmt.Errorf("closed")
	case r.eventc <- ev:
	}

	select {
	case <-r.closed:
		return nil, fmt.Errorf("closed")
	case reply := <-ev.replyc:
		close(ev.replyc)
		return reply, nil
	}
}

func (r *raftNode) Loop() {
	r.logger.Infof("raftNode.loop.start: peers=%v", r.peers)
	for {
		switch r.state {
		case FOLLOWER:
			r.loopFollower()
		case LEADER:
			r.loopLeader()
		case CANDIDATE:
			r.loopCandidate()
		case CLOSED:
			r.logger.Infof("raftNode.loop.closed")
			break
		}
	}
}

func (r *raftNode) loopFollower() {
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
				ev.replyc <- newMessageReply(400, fmt.Sprintf("invalid request %T for follower: %v", ev.msg, ev.msg))
			}
		}
	}
}

// After a candidate raise a rote:
// 它自己赢得选举；
// 另一台机器宣称自己赢得选举；
// 一段时间过后没有赢家
func (r *raftNode) loopCandidate() {
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
				ev.replyc <- newMessageReply(400, fmt.Sprintf("invalid msg for candidate: %T", msg))
			}
		}
	}
}

func (r *raftNode) loopLeader() {
	r.resetLeader()
	heartbeatTicker := r.clock.Ticker(r.heartbeatInterval)
	replyC := make(chan *AppendEntriesReply)

	for r.state == LEADER {
		select {
		case <-r.closed:
			r.closeRaft()

		case <-heartbeatTicker.C:
			r.broadcastAppendEntries(replyC)

		case reply := <- replyC:
			r.processAppendEntriesReply(reply)

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
				ev.replyc <- newMessageReply(400, fmt.Sprintf("invalid msg for leader: %T", msg))
			}
		}
	}
}

func (r *raftNode) processShowStatus(msg *ShowStatusMessage) *ShowStatusReply {
	b := ShowStatusReply{}
	b.Term = r.currentTerm
	b.CommitIndex = r.commitIndex
	b.Peers = r.peers
	b.State = r.state
	return &b
}

func (r *raftNode) processAppendEntries(msg *AppendEntriesMessage) *AppendEntriesReply {
	lastLogIndex, _ := r.storage.MustGetLastLogIndexAndTerm()

	// r.logger.Debugf("raftNode.process-append-entries msg=%#v currentTerm=%d lastLogIndex=%d", msg, currentTerm, lastLogIndex)

	if msg.Term < r.currentTerm {
		return newAppendEntriesReply(false, r.currentTerm, lastLogIndex, "msg.Term < r.currentTerm")
	}

	if msg.Term == r.currentTerm {
		if r.state == LEADER {
			return newAppendEntriesReply(false, r.currentTerm, lastLogIndex, "i'm leader")
		}

		if r.state == CANDIDATE {
			// while waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader
			// if the leader's term is at least as large as the candidate's current term, then the candidate recognizes the leader
			// as legitimate and returns to follower state.
			r.become(FOLLOWER)
		}
	}

	if msg.Term > r.currentTerm {
		r.become(FOLLOWER)
		r.currentTerm = msg.Term
		r.votedFor = ""
		r.mustPersistState()
	}

	if msg.PrevLogIndex > lastLogIndex {
		return newAppendEntriesReply(false, r.currentTerm, lastLogIndex, "log not match")
	}

	if msg.PrevLogIndex < lastLogIndex {
		r.storage.TruncateSince(msg.PrevLogIndex + 1)
	}

	r.storage.AppendLogEntries(msg.LogEntries)
	r.commitIndex = msg.CommitIndex

	lastLogIndex, _ = r.storage.MustGetLastLogIndexAndTerm()
	return newAppendEntriesReply(true, r.currentTerm, lastLogIndex, "success")
}

func (r *raftNode) processRequestVote(msg *RequestVoteMessage) *RequestVoteReply {
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()

	r.logger.Debugf("raftNode.process-request-vote msg=%#v currentTerm=%d votedFor=%s lastLogIndex=%d lastLogTerm=%d", msg, r.currentTerm, r.votedFor, lastLogIndex, lastLogTerm)
	// if the caller's term smaller than mine, refuse
	if msg.Term < r.currentTerm {
		return newRequestVoteReply(false, r.currentTerm, fmt.Sprintf("msg.term: %d < curremtTerm: %d", msg.Term, r.currentTerm))
	}

	// if the term is equal and we've already voted for another candidate
	if msg.Term == r.currentTerm && r.votedFor != "" && r.votedFor != msg.CandidateID {
		return newRequestVoteReply(false, r.currentTerm, fmt.Sprintf("I've already voted another candidate: %s", r.votedFor))
	}

	// if the caller's term bigger than my term: set currentTerm = T, convert to follower
	if msg.Term > r.currentTerm {
		r.become(FOLLOWER)
		r.currentTerm = msg.Term
		r.votedFor = msg.CandidateID
		r.mustPersistState()
	}

	// if the candidate's log is not at least as update as our last log
	if lastLogIndex > msg.LastLogIndex || lastLogTerm > msg.LastLogTerm {
		return newRequestVoteReply(false, r.currentTerm, "candidate's log not at least as update as our last log")
	}

	r.votedFor = msg.CandidateID
	r.mustPersistState()
	return newRequestVoteReply(true, r.currentTerm, "cheers, granted")
}

func (r *raftNode) processCommand(req *CommandMessage) *CommandReply {
	switch req.Command.OpType {
	case kNop:
		return &CommandReply{Value: []byte{}, Message: "nop"}

	case kPut:
		logIndex, _ := r.storage.AppendLogEntriesByCommands([]storage.RaftCommand{req.Command}, r.currentTerm)
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

func (r *raftNode) broadcastAppendEntries(cb chan *AppendEntriesReply) {
	messages, err := r.buildAppendEntriesMessages(r.nextIndex)
	if err != nil {
		return
	}

	r.logger.Debugf("leader.broadcast-heartbeats messages=%v", messages)
	for id, msg := range messages {
		p := r.peers[id]

		go func(msg *AppendEntriesMessage) {
			reply, err := r.rpc.AppendEntries(p, msg)
			if err != nil {
				return
			}
			cb <- reply
		}(msg)
	}
}

func (r *raftNode) processAppendEntriesReply(reply *AppendEntriesReply) {
	// TODO:
	if reply.Term > r.currentTerm {
		// if the receiver has got higher term than myself, turn myself into follower
		r.become(FOLLOWER)
	} else if reply.Success {
		r.nextIndex[p.ID] = reply.LastLogIndex + 1
		r.matchIndex[p.ID] = reply.LastLogIndex
		// TODO: 计算 commitIndex
	} else if !reply.Success {
		if r.nextIndex[p.ID] > 0 {
			r.nextIndex[p.ID]--
		}
	}
}

// runElection broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *raftNode) runElection() bool {
	if r.state != CANDIDATE {
		panic("should be candidate")
	}

	// increase candidate's term and vote for itself
	r.currentTerm += 1
	r.votedFor = r.ID
	r.mustPersistState()
	r.logger.Debugf("raftNode.candidate.vote term=%d votedFor=%s", r.currentTerm, r.ID)

	// send requestVote messages asynchronously, collect the vote results into grantedC
	messages, err := r.buildRequestVoteMessages()
	if err != nil {
		r.logger.Debugf("raftNode.candidate.vote.buildRequestVoteMessages err=%s", err)
		return false
	}

	peers := map[string]Peer{}
	for id, p := range r.peers {
		peers[id] = p
	}

	granted := 0
	for id, msg := range messages {
		p := peers[id]

		resp, err := r.rpc.RequestVote(p, msg)
		r.logger.Debugf("raftNode.candidate.send-request-vote target=%s resp=%#v err=%s", id, resp, err)
		if err != nil {
			continue
		}

		if resp.VoteGranted {
			granted++
		}
	}

	success := (granted+1)*2 > len(peers)+1
	r.logger.Debugf("raftNode.candidate.broadcast-request-vote granted=%d total=%d success=%d", granted+1, len(r.peers)+1, success)
	return success
}

func (r *raftNode) buildRequestVoteMessages() (map[string]*RequestVoteMessage, error) {
	lastLogIndex, lastLogTerm := r.storage.MustGetLastLogIndexAndTerm()
	currentTerm := r.currentTerm

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

func (r *raftNode) buildAppendEntriesMessages(nextLogIndexes map[string]uint64) (map[string]*AppendEntriesMessage, error) {
	messages := map[string]*AppendEntriesMessage{}
	for id, idx := range nextLogIndexes {
		msg := &AppendEntriesMessage{}
		msg.LeaderID = r.ID
		msg.LogEntries = []storage.RaftLogEntry{}
		msg.Term = r.currentTerm
		msg.CommitIndex = r.commitIndex

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

func (r *raftNode) resetLeader() {
	lastLogIndex, _ := r.storage.MustGetLastLogIndexAndTerm()
	for _, p := range r.peers {
		r.nextIndex[p.ID] = lastLogIndex
		r.matchIndex[p.ID] = 0
	}
}

func (r *raftNode) Shutdown() {
	r.logger.Debugf("raftNode.shutdown")
	close(r.closed)
}

func (r *raftNode) closeRaft() {
	r.logger.Infof("raftNode.close-raftNode")
	r.state = CLOSED
	r.storage.Close()
	close(r.eventc)
}

func (r *raftNode) become(s string) {
	r.logger.Debugf("raftNode.set-state state=%s", s)
	// TODO: atomic
	r.state = s
}

func (r *raftNode) mustPersistState() {
	r.storage.MustPutMetaState(storage.RaftMetaState{
		VotedFor: r.votedFor,
		CurrentTerm: r.currentTerm,
		LastApplied: r.lastApplied,
	})
}

func (r *raftNode) mustLoadState() {
	m := r.storage.MustGetMetaState()
	r.currentTerm = m.CurrentTerm
	r.votedFor = m.VotedFor
	r.lastApplied = m.LastApplied
}

func (r *raftNode) newElectionTimer() *clock.Timer {
	return util.NewTimerBetween(r.clock, r.electionTimeout, r.electionTimeout*2)
}
