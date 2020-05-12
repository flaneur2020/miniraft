package raft

import (
	"os"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/stretchr/testify/assert"
)

func makeRaftInstances() (*raftNode, *raftNode, *raftNode, *clock.Mock) {
	os.RemoveAll("/tmp/raftNode-test/")
	os.MkdirAll("/tmp/raftNode-test/", 0777)

	initialPeers := map[string]string{"r1": "192.168.0.1:4501", "r2": "192.168.0.1:4502", "r3": "192.168.0.1:4503"}
	opt1 := &RaftOptions{ID: "r1", StoragePath: "/tmp/raftNode-test/r01", ListenAddr: "0.0.0.0:4501", PeerAddr: "192.168.0.1:4501", InitialPeers: initialPeers}
	opt2 := &RaftOptions{ID: "r2", StoragePath: "/tmp/raftNode-test/r02", ListenAddr: "0.0.0.0:4502", PeerAddr: "192.168.0.1:4502", InitialPeers: initialPeers}
	opt3 := &RaftOptions{ID: "r3", StoragePath: "/tmp/raftNode-test/r03", ListenAddr: "0.0.0.0:4503", PeerAddr: "192.168.0.1:4503", InitialPeers: initialPeers}

	raft1, _ := newRaft(opt1)
	raft2, _ := newRaft(opt2)
	raft3, _ := newRaft(opt3)

	requester := &mockRaftRequester{map[string]*raftNode{"r1": raft1, "r2": raft2, "r3": raft3}}
	clock := clock.NewMock()

	raft1.requester = requester
	raft1.clock = clock

	raft2.requester = requester
	raft2.clock = clock

	raft3.requester = requester
	raft3.clock = clock

	go raft1.Loop()
	go raft2.Loop()
	go raft3.Loop()

	return raft1, raft2, raft3, clock
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
	raft1, raft2, raft3, clock := makeRaftInstances()
	defer func() {
		raft1.Shutdown()
		raft2.Shutdown()
		raft3.Shutdown()
	}()

	req := &AppendEntriesMessage{}
	resp, err := raft1.requester.SendAppendEntries(raft1.peers["r2"], req)
	assert.Equal(t, err, nil)
	assert.Equal(t, resp, &AppendEntriesReply{Term: 0x0, Success: true, Message: "success", LastLogIndex: 0x0})

	assert.Equal(t, raft1.state, FOLLOWER)
	assert.Equal(t, raft2.state, FOLLOWER)
	assert.Equal(t, raft3.state, FOLLOWER)

	clock.Add(5 * time.Second)

	time.Sleep(30)
}
