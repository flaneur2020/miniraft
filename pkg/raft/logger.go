package raft

import (
	"fmt"
	"log"
)

type Logger struct {
	raftID string
	level int
}

var (
	ERROR = 1
	WARN  = 2
	INFO  = 3
	DEBUG = 4
)

func NewRaftLogger(raftID string, level int) *Logger {
	return &Logger{raftID: raftID, level: level}
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	if l.level >= DEBUG {
		format = fmt.Sprintf("[DEBUG] [%s] %s", l.raftID, format)
		log.Printf(format, args...)
	}
}

func (l *Logger) Infof(format string, args ...interface{}) {
	if l.level >= INFO {
		format = fmt.Sprintf("[INFO] [%s] %s", l.raftID, format)
		log.Printf(format, args...)
	}
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	if l.level >= WARN {
		format = fmt.Sprintf("[WARN] [%s] %s", l.raftID, format)
		log.Printf(format, args...)
	}
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	if l.level >= ERROR {
		format = fmt.Sprintf("[ERROR] [%s] %s", l.raftID, format)
		log.Printf(format, args...)
	}
}
