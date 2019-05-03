package raft

type Follower struct {
	*Raft
}

func NewFollower(r *Raft) *Follower {
	return &Follower{r}
}

func (r *Follower) Loop() {
}
