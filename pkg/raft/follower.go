package raft

type Follower struct {
	raft *Raft
}

func (f *Follower) Loop() {
}
