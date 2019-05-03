package raft

type Leader struct {
	*Raft
}

func NewLeader(r *Raft) *Leader {
	return &Leader{r}
}

func (l *Leader) Loop() {
}
