package raft

import (
	"fmt"
	"log"
)

type RaftLogger struct {
	raft  *Raft
	level int
}

var (
	ERROR = 1
	WARN  = 2
	INFO  = 3
	DEBUG = 4
)

func NewRaftLogger(raft *Raft, level int) *RaftLogger {
	return &RaftLogger{raft: raft, level: level}
}

func (l *RaftLogger) Debugf(format string, args ...interface{}) {
	if l.level >= DEBUG {
		format = fmt.Sprintf("[DEBUG] [%s:%s] %s", l.raft.ID, l.raft.state, format)
		log.Printf(format, args...)
	}
}

func (l *RaftLogger) Infof(format string, args ...interface{}) {
	if l.level >= INFO {
		format = fmt.Sprintf("[INFO] [%s:%s] %s", l.raft.ID, l.raft.state, format)
		log.Printf(format, args...)
	}
}

func (l *RaftLogger) Warnf(format string, args ...interface{}) {
	if l.level >= WARN {
		format = fmt.Sprintf("[WARN] [%s:%s] %s", l.raft.ID, l.raft.state, format)
		log.Printf(format, args...)
	}
}

func (l *RaftLogger) Errorf(format string, args ...interface{}) {
	if l.level >= ERROR {
		format = fmt.Sprintf("[ERROR] [%s:%s] %s", l.raft.ID, l.raft.state, format)
		log.Printf(format, args...)
	}
}
