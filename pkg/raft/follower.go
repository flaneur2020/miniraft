package raft

type Follower struct {
	*Raft
}

func (r *Follower) Loop() {
}
