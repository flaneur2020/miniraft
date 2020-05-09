package raft

type RaftLeader struct {
	nextLogIndexes map[string]uint64
	*raft
}

func NewRaftLeader(raft *raft) *RaftLeader {
	l := &RaftLeader{nextLogIndexes: map[string]uint64{}, raft: raft}

	lastLogIndex, _ := l.storage.MustGetLastLogIndexAndTerm()
	for _, p := range l.peers {
		l.nextLogIndexes[p.ID] = lastLogIndex
	}
	return l
}
