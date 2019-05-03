package raft

type Candidate struct {
	*Raft
}

func NewCandidate(r *Raft) *Candidate {
	return &Candidate{Raft: r}
}

func (c *Candidate) Loop() {
}
