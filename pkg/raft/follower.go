package raft

import "log"

type Follower struct {
	*Raft
}

func NewFollower(r *Raft) *Follower {
	return &Follower{r}
}

func (r *Follower) Loop() {
	for r.state == FOLLOWER {
		select {
		case req := <-r.reqc:
			log.Printf("req: %v", req)
		case <-r.closed:
			r.closeRaft()
		}
	}
}
