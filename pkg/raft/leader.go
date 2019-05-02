package raft

type Leader struct {
	*Raft
}

func (l *Leader) Loop() {
}
