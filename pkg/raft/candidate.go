package raft

type Candidate struct {
	raft *Raft
}

func (c *Candidate) Loop() {
}
