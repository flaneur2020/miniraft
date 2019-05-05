package raft

import (
	"math/rand"
	"time"
)

func NewTimerBetween(min, max time.Duration) *time.Timer {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	delta := time.Duration(rand.Int63n(int64(max - min)))
	return time.NewTimer(min + delta)
}
