package raft

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/stretchr/testify/assert"
)

type raftTestContext struct {
	raft1 *raftNode
	raft2 *raftNode
	raft3 *raftNode
}

func newRaftTestContext() *raftTestContext {
	os.RemoveAll("/tmp/raftNode-test/")
	os.MkdirAll("/tmp/raftNode-test/", 0777)

	var (
		electionTimeout   uint64 = 1000
		heartbeatInterval uint64 = 100
	)

	initialPeers := map[string]string{"r1": "192.168.0.1:4501", "r2": "192.168.0.1:4502", "r3": "192.168.0.1:4503"}
	opt1 := &RaftOptions{
		ID:                  "r1",
		StoragePath:         "/tmp/raftNode-test/r01",
		ListenAddr:          "0.0.0.0:4501",
		PeerAddr:            "192.168.0.1:4501",
		InitialPeers:        initialPeers,
		ElectionTimeoutMs:   electionTimeout,
		HeartbeatIntervalMs: heartbeatInterval,
	}
	opt2 := &RaftOptions{
		ID:                  "r2",
		StoragePath:         "/tmp/raftNode-test/r02",
		ListenAddr:          "0.0.0.0:4502",
		PeerAddr:            "192.168.0.1:4502",
		InitialPeers:        initialPeers,
		ElectionTimeoutMs:   electionTimeout,
		HeartbeatIntervalMs: heartbeatInterval,
	}
	opt3 := &RaftOptions{
		ID:                  "r3",
		StoragePath:         "/tmp/raftNode-test/r03",
		ListenAddr:          "0.0.0.0:4503",
		PeerAddr:            "192.168.0.1:4503",
		InitialPeers:        initialPeers,
		ElectionTimeoutMs:   electionTimeout,
		HeartbeatIntervalMs: heartbeatInterval,
	}

	raft1, _ := newRaft(opt1)
	raft2, _ := newRaft(opt2)
	raft3, _ := newRaft(opt3)

	requester := &mockRaftRequester{map[string]*raftNode{"r1": raft1, "r2": raft2, "r3": raft3}}
	clock1 := clock.New()
	clock2 := clock.New()
	clock3 := clock.New()

	raft1.rpc = requester
	raft1.clock = clock1

	raft2.rpc = requester
	raft2.clock = clock2

	raft3.rpc = requester
	raft3.clock = clock3

	go raft1.Loop()
	go raft2.Loop()
	go raft3.Loop()

	return &raftTestContext{
		raft1: raft1,
		raft2: raft2,
		raft3: raft3,
	}
}

func (c *raftTestContext) Shutdown() {
	log.Printf("raftTestContext.shutdown")
	c.raft1.Shutdown()
	c.raft2.Shutdown()
	c.raft3.Shutdown()
}

func Test_NewRaft(t *testing.T) {
	opt := &RaftOptions{
		ID:          "r1",
		StoragePath: "/tmp/raft01",
		ListenAddr:  "0.0.0.0:4501",
		PeerAddr:    "192.168.0.1:4501",
		InitialPeers: map[string]string{
			"r1": "192.168.0.1:4501",
			"r2": "192.168.0.1:4502",
			"r3": "192.168.0.1:4503",
		},
	}
	r, err := newRaft(opt)
	assert.Nil(t, err)
	assert.Equal(t, r.ID, opt.ID)
	assert.Equal(t, r.state, FOLLOWER)
	assert.Equal(t, len(r.peers), 2)
	assert.Equal(t, r.peers["r2"], Peer{ID: "r2", Addr: "192.168.0.1:4502"})
}

func Test_RaftRequest(t *testing.T) {
	// go test github.com/fleurer/miniraft/pkg/raftNode -run Test_RaftRequest  -v
	tc := newRaftTestContext()
	defer tc.Shutdown()

	req := &AppendEntriesMessage{}
	reply, err := tc.raft1.rpc.AppendEntries(tc.raft1.peers["r2"], req)
	assert.Equal(t, err, nil)
	assert.Equal(t, reply, &AppendEntriesReply{Term: 0x0, Success: true, PeerID: "r2", Message: "success", LastLogIndex: 0x0})

	assert.Equal(t, tc.raft1.state, FOLLOWER)
	assert.Equal(t, tc.raft2.state, FOLLOWER)
	assert.Equal(t, tc.raft3.state, FOLLOWER)

	time.Sleep(2 * time.Second)

	assert.True(t, tc.raft1.state == LEADER || tc.raft2.state == LEADER || tc.raft3.state == LEADER)
}

func Test_calculateLeaderCommitIndex(t *testing.T) {
	dt := []struct {
		m    map[string]uint64
		want uint64
	}{
		{map[string]uint64{"n1": 1, "n2": 1, "n3": 2}, 1},
		{map[string]uint64{"n1": 2, "n2": 1, "n3": 2}, 2},
		{map[string]uint64{"n1": 1, "n2": 1, "n3": 2, "n4": 2}, 1},
		{map[string]uint64{"n1": 1, "n2": 1, "n3": 2, "n4": 3}, 1},
		{map[string]uint64{"n1": 1, "n2": 1, "n3": 2, "n4": 2, "n5": 2}, 2},
	}

	for _, d := range dt {
		got := calculateLeaderCommitIndex(d.m)
		assert.Equal(t, got, d.want)
	}
}
