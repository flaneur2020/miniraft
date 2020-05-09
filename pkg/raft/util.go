package raft

import (
	"math/rand"
	"time"

	"github.com/facebookgo/clock"
)

func NewTimerBetween(c clock.Clock, min, max time.Duration) *clock.Timer {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	delta := time.Duration(rand.Int63n(int64(max - min)))
	return c.Timer(min + delta)
}
