package raft

type RaftLeader struct {
	nextLogIndexes map[string]uint64
	*Raft
}

func NewRaftLeader(raft *Raft) *RaftLeader {
	l := &RaftLeader{nextLogIndexes: map[string]uint64{}, Raft: raft}

	lastLogIndex, _ := l.storage.MustGetLastLogIndexAndTerm()
	for _, p := range l.peers {
		l.nextLogIndexes[p.ID] = lastLogIndex
	}
	return l
}
