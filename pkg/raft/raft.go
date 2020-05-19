package raft

import (
	"errors"
	"fmt"
	"sort"
	"sync"
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

var (
	ErrClosed = errors.New("closed")
)

type Peer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type RaftNode interface {
	// start the raft loop
	Start()

	// close the raft loop
	Stop()

	// pass the message to raft loop
	Do(msg RaftMessage) (RaftReply, error)
}

type RaftOptions struct {
	ID          string `json:"id"`
	StoragePath string `json:"storagePath"`

	ListenAddr   string            `json:"listenAddr"`
	PeerAddr     string            `json:"peerAddr"`
	InitialPeers map[string]string `json:"initialPeers"`

	TickIntervalMs uint `json:"tickIntervalMs"`
	ElectionTicks  uint `json:"electionTicks"`
	HeartbeatTicks uint `json:"heartbeatTicks"`
}

type raftNode struct {
	ID    string
	state string
	peers map[string]Peer

	// volatile state on leaders
	nextIndex  map[string]uint64
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

	eventc       chan *raftEV
	closed       chan struct{}
	routineGroup sync.WaitGroup
}

type raftEV struct {
	msg   RaftMessage
	c     chan struct{}
	reply RaftReply
	err   error
}

func newRaftEV(msg RaftMessage) *raftEV {
	return &raftEV{
		msg:   msg,
		c:     make(chan struct{}, 1),
		reply: nil,
		err:   nil,
	}
}

func (ev *raftEV) Done(reply RaftReply, err error) {
	ev.reply = reply
	ev.err = err
	close(ev.c)
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

	storage, err := storage.NewRaftStorage(opt.StoragePath, opt.ID)
	if err != nil {
		return nil, err
	}

	r := &raftNode{}
	r.ID = opt.ID
	r.state = FOLLOWER
	r.electionTimeout = time.Duration(opt.ElectionTicks*opt.TickIntervalMs) * time.Millisecond
	r.heartbeatInterval = time.Duration(opt.HeartbeatTicks*opt.TickIntervalMs) * time.Millisecond
	r.peers = peers
	r.storage = storage
	r.votedFor = ""
	r.nextIndex = map[string]uint64{}
	r.matchIndex = map[string]uint64{}
	r.clock = clock.New()
	r.logger = util.NewRaftLogger(r.ID, util.DEBUG)
	r.rpc = NewRaftRPC(r.logger)
	r.eventc = make(chan *raftEV)
	r.closed = make(chan struct{})
	r.mustLoadState()
	return r, nil
}

func (r *raftNode) Start() {
	r.routineGroup.Add(1)
	go func() {
		r.loop()
		r.routineGroup.Done()
	}()
}

func (r *raftNode) Stop() {
	r.logger.Debugf("raft.stop")

	select {
	case <-r.closed:
		return
	default:
		close(r.closed)
	}

	r.routineGroup.Wait()
}

func (r *raftNode) closeRaft() {
	r.state = CLOSED
	r.storage.Close()
	// close(r.eventc)
}

func (r *raftNode) Do(msg RaftMessage) (RaftReply, error) {
	ev := newRaftEV(msg)

	select {
	case <-r.closed:
		return nil, ErrClosed
	case r.eventc <- ev:
	}

	select {
	case <-r.closed:
		return nil, ErrClosed
	case <-ev.c:
		return ev.reply, ev.err
	}
}

func (r *raftNode) dispatch(msg RaftMessage) error {
	ev := newRaftEV(msg)

	select {
	case <-r.closed:
		return ErrClosed
	case r.eventc <- ev:
		return nil
	}
}

func (r *raftNode) loop() {
	r.logger.Infof("raft.loop.start: peers=%v", r.peers)

	for r.state != CLOSED {
		switch r.state {
		case FOLLOWER:
			r.loopFollower()
		case LEADER:
			r.loopLeader()
		case CANDIDATE:
			r.loopCandidate()
		case CLOSED:
		}
	}

	r.logger.Infof("raft.loop.closed")
}

func (r *raftNode) loopFollower() {
	electionTimer := r.newElectionTimer()

	for r.state == FOLLOWER {
		select {
		case <-electionTimer.C:
			go r.dispatch(&ElectionTimeoutMsg{})

		case <-r.closed:
			r.closeRaft()

		case ev := <-r.eventc:
			switch msg := ev.msg.(type) {
			case *ElectionTimeoutMsg:
				r.logger.Infof("follower.loop.electionTimeout")
				r.become(CANDIDATE)

			case *AppendEntriesMsg:
				reply := r.processAppendEntries(msg)
				ev.Done(reply, nil)
				electionTimer = r.newElectionTimer()

			case *RequestVoteMsg:
				reply := r.processRequestVote(msg)
				ev.Done(reply, nil)

			case *ShowStatusMsg:
				reply := r.processShowStatus(msg)
				ev.Done(reply, nil)

			default:
				reply := newMessageReply(400, fmt.Sprintf("invalid request %T for follower: %v", ev.msg, ev.msg))
				ev.Done(reply, nil)
			}
		}
	}
}

// After a candidate raise a rote:
// 它自己赢得选举；
// 另一台机器宣称自己赢得选举；
// 一段时间过后没有赢家
func (r *raftNode) loopCandidate() {
	electionTimer := r.newElectionTimer()
	replyC := make(chan *RequestVoteReply, len(r.peers))
	granted := 0
	go r.dispatch(&ElectionTimeoutMsg{})

	for r.state == CANDIDATE {
		select {
		case <-r.closed:
			r.closeRaft()

		case <-electionTimer.C:
			go r.dispatch(&ElectionTimeoutMsg{})

		case reply := <-replyC:
			go r.dispatch(&RequestVoteResultMsg{reply: reply})

		case ev := <-r.eventc:
			switch msg := ev.msg.(type) {
			case *ElectionTimeoutMsg:
				r.runElection(replyC)
				electionTimer = r.newElectionTimer()
				granted = 0

			case *RequestVoteResultMsg:
				if msg.reply != nil && msg.reply.VoteGranted {
					granted++
				}
				success := (granted+1)*2 > len(r.peers)+1
				r.logger.Debugf("raft.candidate.broadcast-request-vote term=%d granted=%d total=%d success=%v", r.currentTerm, granted+1, len(r.peers)+1, success)
				if success {
					r.become(LEADER)
					continue
				}

			case *RequestVoteMsg:
				reply := r.processRequestVote(msg)
				ev.Done(reply, nil)

			case *ShowStatusMsg:
				reply := r.processShowStatus(msg)
				ev.Done(reply, nil)

			default:
				reply := newMessageReply(400, fmt.Sprintf("invalid msg for candidate: %T", msg))
				ev.Done(reply, nil)
			}
		}
	}
}

func (r *raftNode) loopLeader() {
	r.resetLeader()
	heartbeatTicker := r.clock.Ticker(r.heartbeatInterval)
	replyC := make(chan *AppendEntriesReply, len(r.peers))

	// a leader should append a nop log entry immediately
	logIndex, _ := r.storage.Append(storage.NopCommand, r.currentTerm)
	r.logger.Infof("leader.append-nop term=%v logIndex=%v", r.currentTerm, logIndex)

	for r.state == LEADER {
		select {
		case <-r.closed:
			r.closeRaft()

		case <-heartbeatTicker.C:
			go r.dispatch(&HeartbeatTimeoutMsg{})

		case reply := <-replyC:
			go r.dispatch(&AppendEntriesResultMsg{reply: reply})

		case ev := <-r.eventc:
			switch msg := ev.msg.(type) {
			case *HeartbeatTimeoutMsg:
				r.broadcastAppendEntries(replyC)
				ev.Done(nil, nil)

			case *AppendEntriesResultMsg:
				r.processAppendEntriesReply(msg.reply)
				ev.Done(nil, nil)

			case *AppendEntriesMsg:
				reply := r.processAppendEntries(msg)
				ev.Done(reply, nil)

			case *RequestVoteMsg:
				reply := r.processRequestVote(msg)
				ev.Done(reply, nil)

			case *ShowStatusMsg:
				reply := r.processShowStatus(msg)
				ev.Done(reply, nil)

			case *CommandMessage:
				reply := r.processCommand(msg)
				ev.Done(reply, nil)

			default:
				reply := newMessageReply(400, fmt.Sprintf("invalid msg for leader: %T", msg))
				ev.Done(reply, nil)
			}
		}
	}
}

func (r *raftNode) processShowStatus(msg *ShowStatusMsg) *ShowStatusReply {
	b := ShowStatusReply{}
	b.Term = r.currentTerm
	b.CommitIndex = r.commitIndex
	b.Peers = r.peers
	b.State = r.state
	return &b
}

func (r *raftNode) processAppendEntries(msg *AppendEntriesMsg) *AppendEntriesReply {
	lastIndex, _ := r.storage.MustLastIndexAndTerm()

	// r.logger.Debugf("raft.process-append-entries msg=%#v currentTerm=%d lastIndex=%d", msg, r.currentTerm, lastIndex)

	if msg.Term < r.currentTerm {
		return newAppendEntriesReply(false, r.currentTerm, lastIndex, r.ID, "msg.Term < r.currentTerm")
	}

	if msg.Term == r.currentTerm {
		if r.state == LEADER {
			return newAppendEntriesReply(false, r.currentTerm, lastIndex, r.ID, "i'm leader")
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

	if msg.PrevLogIndex > lastIndex {
		return newAppendEntriesReply(false, r.currentTerm, lastIndex, r.ID, fmt.Sprintf("log not match: msg.prevLogIndex(%v) > lastIndex(%v)", msg.PrevLogIndex, lastIndex))
	}

	if msg.PrevLogIndex < lastIndex {
		count, err := r.storage.TruncateSince(msg.PrevLogIndex + 1)
		if err != nil {
			panic(err)
		}

		r.logger.Infof("raft.truncate-since msg.PrevLogIndex=%v lastIndex=%v count=%v", msg.PrevLogIndex, lastIndex, count)
	}

	if err := r.storage.BulkAppend(msg.LogEntries); err != nil {
		panic(err)
	}

	lastIndex, _ = r.storage.MustLastIndexAndTerm()
	r.commitIndex = msg.CommitIndex
	r.applyLogs()

	return newAppendEntriesReply(true, r.currentTerm, lastIndex, r.ID, "success")
}

func (r *raftNode) processRequestVote(msg *RequestVoteMsg) *RequestVoteReply {
	lastIndex, lastLogTerm := r.storage.MustLastIndexAndTerm()

	r.logger.Debugf("raft.process-request-vote msg=%#v currentTerm=%d votedFor=%s lastIndex=%d lastLogTerm=%d", msg, r.currentTerm, r.votedFor, lastIndex, lastLogTerm)

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
	if lastIndex > msg.LastLogIndex || lastLogTerm > msg.LastLogTerm {
		return newRequestVoteReply(false, r.currentTerm, "candidate's log not at least as update as our last log")
	}

	r.votedFor = msg.CandidateID
	r.mustPersistState()
	return newRequestVoteReply(true, r.currentTerm, "cheers, granted")
}

func (r *raftNode) processRequestVoteResult(reply *RequestVoteReply) {

}

func (r *raftNode) processCommand(req *CommandMessage) *CommandReply {
	switch req.Command.Type {
	case storage.NopCommandType:
		return &CommandReply{Value: []byte{}, Message: "nop"}

	case storage.PutCommandType:
		logIndex, err := r.storage.Append(req.Command, r.currentTerm)
		if err != nil {
			panic(err)
		}

		// TODO: await logIndex got commit
		return &CommandReply{Value: []byte{}, Message: fmt.Sprintf("logIndex: %d", logIndex)}

	case storage.GetCommandType:
		v, exists := r.storage.MustGetKV(req.Command.Key)
		if !exists {
			return &CommandReply{Value: nil, Message: "not found"}
		}
		return &CommandReply{Value: v, Message: "success"}

	default:
		panic(fmt.Sprintf("unexpected command type: %s", req.Command.Type))

	}
}

func (r *raftNode) broadcastAppendEntries(cb chan *AppendEntriesReply) {
	messages, err := r.buildAppendEntriesMessages(r.nextIndex)
	if err != nil {
		return
	}

	lastIndex, _ := r.storage.MustLastIndexAndTerm()
	r.logger.Debugf("leader.broadcast-heartbeats nextIndex=%v matchIndex=%v term=%v commitIndex=%v lastIndex=%v", r.nextIndex, r.matchIndex, r.currentTerm, r.commitIndex, lastIndex)

	for id, msg := range messages {
		p := r.peers[id]

		r.routineGroup.Add(1)
		go func(p Peer, msg *AppendEntriesMsg) {
			defer r.routineGroup.Done()

			reply, err := r.rpc.AppendEntries(p, msg)
			if err != nil {
				return
			}
			cb <- reply
		}(p, msg)
	}
}

func (r *raftNode) processAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term > r.currentTerm {
		// if the receiver has got higher term than myself, turn myself into follower
		r.become(FOLLOWER)
		return
	}

	if !reply.Success {
		r.logger.Debugf("leader.process-append-entries-reply not-success msg=%s", reply.Message)
		if r.nextIndex[reply.PeerID] > 0 {
			r.nextIndex[reply.PeerID]--
		}
		return
	}

	r.nextIndex[reply.PeerID] = reply.LastLogIndex + 1
	r.matchIndex[reply.PeerID] = reply.LastLogIndex

	commitIndex := calculateLeaderCommitIndex(r.matchIndex)
	if r.commitIndex < commitIndex {
		r.commitIndex = commitIndex
	}

	r.applyLogs()
}

// runElection broadcasts the requestVote messages, and collect the vote result asynchronously.
func (r *raftNode) runElection(cb chan *RequestVoteReply) {
	if r.state != CANDIDATE {
		panic("should be candidate")
	}

	// increase candidate's term and vote for itself
	r.currentTerm += 1
	r.votedFor = r.ID
	r.mustPersistState()
	r.logger.Debugf("raft.candidate.vote term=%d votedFor=%s", r.currentTerm, r.ID)

	// send requestVote messages asynchronously, collect the vote results into grantedC
	messages, err := r.buildRequestVoteMessages()
	if err != nil {
		r.logger.Debugf("raft.candidate.vote.buildRequestVoteMessages err=%s", err)
		return
	}

	for id, msg := range messages {
		p := r.peers[id]

		if id == r.ID {
			panic("do not request vote for myself")
		}

		r.routineGroup.Add(1)
		go func(p Peer, msg *RequestVoteMsg) {
			defer r.routineGroup.Done()

			reply, err := r.rpc.RequestVote(p, msg)
			cb <- reply

			r.logger.Debugf("raft.candidate.request-vote target=%s resp=%#v err=%s", id, reply, err)
		}(p, msg)
	}
}

func (r *raftNode) buildRequestVoteMessages() (map[string]*RequestVoteMsg, error) {
	lastIndex, lastLogTerm := r.storage.MustLastIndexAndTerm()
	currentTerm := r.currentTerm

	messages := map[string]*RequestVoteMsg{}
	for id := range r.peers {
		if id == r.ID {
			continue
		}
		msg := RequestVoteMsg{}
		msg.CandidateID = r.ID
		msg.LastLogIndex = lastIndex
		msg.LastLogTerm = lastLogTerm
		msg.Term = currentTerm
		messages[id] = &msg
	}
	return messages, nil
}

func (r *raftNode) buildAppendEntriesMessages(nextLogIndexes map[string]uint64) (map[string]*AppendEntriesMsg, error) {
	var (
		messages = map[string]*AppendEntriesMsg{}
		err      error
	)
	for id, idx := range nextLogIndexes {
		msg := &AppendEntriesMsg{}
		msg.LeaderID = r.ID
		msg.LogEntries = []storage.RaftLogEntry{}
		msg.Term = r.currentTerm
		msg.CommitIndex = r.commitIndex

		if idx == 0 {
			msg.PrevLogIndex = 0
			msg.PrevLogTerm = 0
			msg.LogEntries, err = r.storage.EntriesSince(0)
			if err != nil {
				panic(err)
			}
		} else {
			logEntries, err := r.storage.EntriesSince(idx - 1)
			if err != nil {
				panic(err)
			}

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
	lastIndex, _ := r.storage.MustLastIndexAndTerm()
	for _, p := range r.peers {
		r.nextIndex[p.ID] = lastIndex + 1
		r.matchIndex[p.ID] = 0
	}
}

func (r *raftNode) become(s string) {
	r.logger.Debugf("raft.set-state state=%s", s)
	r.state = s
}

func (r *raftNode) applyLogs() {
	// TODO: optimize this
	entries, err := r.storage.EntriesSince(r.lastApplied + 1)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.Index > r.commitIndex {
			break
		}

		switch entry.Command.Type {
		case storage.PutCommandType:
			r.storage.MustPutKV(entry.Command.Key, entry.Command.Value)
		}

		r.lastApplied++
		r.logger.Debugf("%s.apply-log lastApplied=%d commitIndex=%d command=%#v", r.state, r.lastApplied, r.commitIndex, entry.Command)
		r.mustPersistState()
	}
}

func (r *raftNode) mustPersistState() {
	err := r.storage.PutHardState(&storage.RaftHardState{
		VotedFor:    r.votedFor,
		CurrentTerm: r.currentTerm,
		LastApplied: r.lastApplied,
	})
	if err != nil {
		panic(err)
	}
}

func (r *raftNode) mustLoadState() {
	m, err := r.storage.HardState()
	if err != nil {
		panic(err)
	}
	r.currentTerm = m.CurrentTerm
	r.votedFor = m.VotedFor
	r.lastApplied = m.LastApplied
}

func (r *raftNode) newElectionTimer() *clock.Timer {
	return util.NewTimerBetween(r.clock, r.electionTimeout, r.electionTimeout*2)
}

func calculateLeaderCommitIndex(matchIndex map[string]uint64) uint64 {
	indices := []uint64{}
	for _, idx := range matchIndex {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] >= indices[j] })
	return indices[len(indices)/2]
}
