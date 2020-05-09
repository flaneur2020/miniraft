package storage

import (
	"github.com/Fleurer/miniraft/pkg/raft"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetLastLogEntry(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test01", "raft-test01")
	assert.Nil(t, err)
	es := []raft.RaftLogEntry{
		{Command: raft.RaftCommand{}, Term: 0, Index: 1},
		{Command: raft.RaftCommand{}, Term: 0, Index: 2},
		{Command: raft.RaftCommand{}, Term: 0, Index: 0},
	}
	err = s.AppendLogEntries(es)
	assert.Nil(t, err)

	le, err := s.getLastLogEntry()
	assert.Nil(t, err)
	assert.Equal(t, le.Index, uint64(2))
}

func Test_GetLogEntriesSince(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test02", "raft-test01")
	assert.Nil(t, err)
	es := []raft.RaftLogEntry{
		{Command: raft.RaftCommand{}, Term: 0, Index: 1},
		{Command: raft.RaftCommand{}, Term: 0, Index: 0},
		{Command: raft.RaftCommand{}, Term: 0, Index: 2},
	}
	err = s.AppendLogEntries(es)
	assert.Nil(t, err)

	es = s.MustGetLogEntriesSince(3)
	assert.Equal(t, len(es), 0)

	es = s.MustGetLogEntriesSince(0)
	assert.Equal(t, len(es), 3)

	es = s.MustGetLogEntriesSince(2)
	assert.Equal(t, len(es), 1)
}
