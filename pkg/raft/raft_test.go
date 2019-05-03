package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	r, err := NewRaft(opt)
	assert.Nil(t, err)
	assert.Equal(t, r.ID, opt.ID)
	assert.Equal(t, r.peers["r1"], Peer{ID: "r1", Addr: "192.168.0.1:4501"})
}
