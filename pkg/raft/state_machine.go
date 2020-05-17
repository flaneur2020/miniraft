package raft

import "github.com/Fleurer/miniraft/pkg/storage"

type StateMachine interface {
	Get() ([]byte, error)

	Put([]byte) error

	LastApplied() uint64

	Apply(logIndex uint64, cmd storage.RaftCommand) (uint64, error)
}

type KVStateMachine struct {
}
