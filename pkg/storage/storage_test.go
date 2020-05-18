package storage

import (
	"fmt"
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
	err = s.BulkAppend(es)
	assert.Nil(t, err)
}

func TestRaftStorage_MustGetLastLogIndexAndTerm(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test03", "raft-test01")
	s.Destroy()
	defer s.Close()
	assert.Nil(t, err)

	lastIndex, lastTerm := s.MustLastIndexAndTerm()
	assert.Equal(t, uint64(0), lastIndex)
	assert.Equal(t, uint64(0), lastTerm)

	s.Append(NopCommand, 1)
	lastIndex, lastTerm = s.MustLastIndexAndTerm()
	assert.Equal(t, uint64(1), lastIndex)
	assert.Equal(t, uint64(1), lastTerm)
}

func TestRaftStorage_TruncateSince(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test02", "raft-test01")
	s.Destroy()
	defer s.Close()
	assert.Nil(t, err)

	s.Append(NopCommand, 1)
	s.Append(NopCommand, 1)
	s.Append(NopCommand, 1)

	lastIndex, _ := s.MustLastIndexAndTerm()
	assert.Equal(t, uint64(3), lastIndex)

	s.TruncateSince(3)
	lastIndex, _ = s.MustLastIndexAndTerm()
	assert.Equal(t, uint64(2), lastIndex)

	s.TruncateSince(2)
	lastIndex, _ = s.MustLastIndexAndTerm()
	assert.Equal(t, uint64(1), lastIndex)

	s.TruncateSince(0)
	lastIndex, _ = s.MustLastIndexAndTerm()
	assert.Equal(t, uint64(0), lastIndex)
}

func Test_GetLogEntriesSince(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test02", "raft-test01")
	assert.Nilf(t, err, fmt.Sprintf("%s", err))
	assert.NotNil(t, s)
	defer s.Close()

	es := []RaftLogEntry{
		{Command: RaftCommand{}, Term: 0, Index: 1},
		{Command: RaftCommand{}, Term: 0, Index: 0},
		{Command: RaftCommand{}, Term: 0, Index: 2},
	}
	err = s.BulkAppend(es)
	assert.Nil(t, err)

	es, _ = s.EntriesSince(3)
	assert.Equal(t, len(es), 0)

	es, _ = s.EntriesSince(0)
	assert.Equal(t, len(es), 3)

	es, _ = s.EntriesSince(2)
	assert.Equal(t, len(es), 1)
}
