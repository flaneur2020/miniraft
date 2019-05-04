package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_AppendEntries(t *testing.T) {
	s, err := NewRaftStorage("/tmp/test01", "raft-test01")
	assert.Nil(t, err)
	es := []RaftLogEntry{
		{OpType: 0, Term: 0, Index: 0},
		{OpType: 0, Term: 0, Index: 1},
		{OpType: 0, Term: 0, Index: 2},
	}
	err = s.AppendLogEntries(es)
	assert.Nil(t, err)

	le, err := s.GetLastLogEntry()
	assert.Nil(t, err)
	assert.Equal(t, le.Index, uint64(2))
}
