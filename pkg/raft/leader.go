package raft

type Leader struct {
	raft *Raft
}

func (l *Leader) Loop() {
}
