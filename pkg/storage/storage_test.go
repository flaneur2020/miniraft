package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetLastLogEntry(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test01", "raft-test01")
	assert.Nil(t, err)
	es := []RaftLogEntry{
		{Command: RaftCommand{}, Term: 0, Index: 1},
		{Command: RaftCommand{}, Term: 0, Index: 2},
		{Command: RaftCommand{}, Term: 0, Index: 0},
	}
	err = s.AppendBulkLogEntries(es)
	assert.Nil(t, err)
}

func TestRaftStorage_MustGetLastLogIndexAndTerm(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test03", "raft-test01")
	s.Reset()
	assert.Nil(t, err)

	lastIndex, lastTerm := s.MustGetLastLogIndexAndTerm()
	assert.Equal(t, uint64(0), lastIndex)
	assert.Equal(t, uint64(0), lastTerm)

	s.AppendLogEntry(NopCommand, 1)
	lastIndex, lastTerm = s.MustGetLastLogIndexAndTerm()
	assert.Equal(t, uint64(1), lastIndex)
	assert.Equal(t, uint64(1), lastTerm)
}

func TestRaftStorage_TruncateSince(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test02", "raft-test01")
	s.Reset()
	assert.Nil(t, err)

	s.AppendLogEntry(NopCommand, 1)
	s.AppendLogEntry(NopCommand, 1)
	s.AppendLogEntry(NopCommand, 1)

	lastIndex, _ := s.MustGetLastLogIndexAndTerm()
	assert.Equal(t, uint64(3), lastIndex)

	s.TruncateSince(3)
	lastIndex, _ = s.MustGetLastLogIndexAndTerm()
	assert.Equal(t, uint64(2), lastIndex)

	s.TruncateSince(2)
	lastIndex, _ = s.MustGetLastLogIndexAndTerm()
	assert.Equal(t, uint64(1), lastIndex)

	s.TruncateSince(0)
	lastIndex, _ = s.MustGetLastLogIndexAndTerm()
	assert.Equal(t, uint64(0), lastIndex)
}

func Test_GetLogEntriesSince(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test02", "raft-test01")
	assert.Nil(t, err)
	es := []RaftLogEntry{
		{Command: RaftCommand{}, Term: 0, Index: 1},
		{Command: RaftCommand{}, Term: 0, Index: 0},
		{Command: RaftCommand{}, Term: 0, Index: 2},
	}
	err = s.AppendBulkLogEntries(es)
	assert.Nil(t, err)

	es = s.MustGetLogEntriesSince(3)
	assert.Equal(t, len(es), 0)

	es = s.MustGetLogEntriesSince(0)
	assert.Equal(t, len(es), 3)

	es = s.MustGetLogEntriesSince(2)
	assert.Equal(t, len(es), 1)
}
