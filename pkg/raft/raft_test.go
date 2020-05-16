package raft

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Fleurer/miniraft/pkg/storage"

	"github.com/stretchr/testify/assert"
)

type raftTestContext struct {
	nodes []*raftNode
}

func newRaftTestContext() *raftTestContext {
	os.RemoveAll("/tmp/raftNode-test/")
	os.MkdirAll("/tmp/raftNode-test/", 0777)

	var (
		electionTimeout   uint64 = 900
		heartbeatInterval uint64 = 100
	)

	initialPeers := map[string]string{"r0": "192.168.0.1:4500", "r1": "192.168.0.1:4501", "r2": "192.168.0.1:4502"}

	opts := [3]*RaftOptions{}
	for i := 0; i < 3; i++ {
		opts[i] = &RaftOptions{
			ID:                  fmt.Sprintf("r%d", i),
			StoragePath:         fmt.Sprintf("/tmp/raftNode-test/r0%d", i),
			ListenAddr:          fmt.Sprintf("0.0.0.0:450%d", i),
			PeerAddr:            fmt.Sprintf("192.168.0.1:450%d", i),
			InitialPeers:        initialPeers,
			ElectionTimeoutMs:   electionTimeout,
			HeartbeatIntervalMs: heartbeatInterval,
		}
	}

	nodes := []*raftNode{}
	for i := 0; i < 3; i++ {
		node, _ := newRaft(opts[i])
		nodes = append(nodes, node)
	}

	requester := &mockRaftRequester{
		rafts: map[string]*raftNode{
			"r0": nodes[0],
			"r1": nodes[1],
			"r2": nodes[2],
		},
	}

	for i := 0; i < 3; i++ {
		nodes[i].rpc = requester
		go nodes[i].Loop()
	}

	return &raftTestContext{nodes}
}

func (c *raftTestContext) Leader() *raftNode {
	for _, node := range c.nodes {
		if node.state == LEADER {
			return node
		}
	}
	return nil
}

func (c *raftTestContext) Followers() []*raftNode {
	nodes := []*raftNode{}
	for _, node := range c.nodes {
		if node.state == FOLLOWER {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (c *raftTestContext) Candidates() []*raftNode {
	nodes := []*raftNode{}
	for _, node := range c.nodes {
		if node.state == CANDIDATE {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (c *raftTestContext) Node(i int) *raftNode {
	return c.nodes[i-1]
}

func (c *raftTestContext) Shutdown() {
	log.Printf("raftTestContext.shutdown")
	for _, node := range c.nodes {
		node.Stop()
	}
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

func Test_Raft_Election(t *testing.T) {
	tc := newRaftTestContext()
	defer tc.Shutdown()

	assert.True(t, tc.Leader() == nil)
	assert.Equal(t, 3, len(tc.Followers()))

	time.Sleep(2 * time.Second)
	assert.True(t, tc.Leader() != nil)
	assert.Equal(t, 2, len(tc.Followers()))

	rl := tc.Leader()
	rl.Stop()
	time.Sleep(2 * time.Second)
	assert.True(t, tc.Leader() != nil)
	assert.Equal(t, 1, len(tc.Followers()))

	rl = tc.Leader()
	rl.Stop()
	time.Sleep(2 * time.Second)
	assert.True(t, tc.Leader() == nil)
	assert.Equal(t, 1, len(tc.Candidates()))
}

func TestRaftNode_Replication(t *testing.T) {
	tc := newRaftTestContext()
	defer tc.Shutdown()

	time.Sleep(2 * time.Second)
	assert.True(t, tc.Leader() != nil)
	assert.Equal(t, 2, len(tc.Followers()))

	rl := tc.Leader()
	rl.Process(&CommandMessage{storage.RaftCommand{
		Type:  storage.PutCommandType,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}})

	time.Sleep(1 * time.Second)
	rf := tc.Followers()[0]
	got, _ := rf.storage.MustGetKV([]byte("key1"))
	want := []byte("value1")
	assert.Equal(t, want, got)
}

func TestRaftNode_Request(t *testing.T) {
	tc := newRaftTestContext()
	defer tc.Shutdown()

	req := &AppendEntriesMessage{}
	rf1 := tc.Followers()[0]
	rf2 := tc.Followers()[1]
	got, err := rf1.rpc.AppendEntries(rf1.peers[rf2.ID], req)
	// want := &AppendEntriesReply{Term: 0x0, Success: true, PeerID: "r1", Message: "success", LastLogIndex: 0x0}
	assert.Equal(t, nil, err)
	assert.Equal(t, true, got.Success)
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
