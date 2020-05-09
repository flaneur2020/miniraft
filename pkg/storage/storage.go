package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	lerrors "github.com/syndtr/goleveldb/leveldb/errors"
	lutil "github.com/syndtr/goleveldb/leveldb/util"
)

const (
	kCurrentTerm = "m:current-term"
	kVotedFor    = "m:voted-for"
	kCommitIndex = "m:commit-index"
	kLastApplied = "m:last-applied"
	kLogEntries  = "l:log-entries"
)

type RaftLogEntry struct {
	Term    uint64      `json:"term"`
	Index   uint64      `json:"index"`
	Command RaftCommand `json:"command"`
}

type RaftCommand struct {
	OpType string `json:"opType"`
	Key    []byte `json:"key"`
	Value  []byte `json:"value,omitempty"`
}

type RaftStorage interface {
	MustGetKV([]byte) ([]byte, bool)
	MustPutKV([]byte, []byte)
	MustDeleteKV([]byte)

	MustGetCurrentTerm() uint64
	MustGetCommitIndex() uint64
	MustGetLastApplied() uint64
	MustGetVotedFor() string
	MustGetLastLogIndexAndTerm() (uint64, uint64)
	MustGetLogEntriesSince(index uint64) []RaftLogEntry

	PutCurrentTerm(uint64) error
	PutLastApplied(uint64) error
	PutCommitIndex(uint64) error
	PutVotedFor(string) error
	AppendLogEntries(entries []RaftLogEntry) error
	AppendLogEntriesByCommands(commands []RaftCommand) (uint64, error)
	TruncateSince(index uint64)

	Reset()
	Close()
}

var _ RaftStorage = &raftStorage{}

type raftStorage struct {
	db        *leveldb.DB
	keyPrefix string
	path      string
}

func NewRaftStorage(path string, keyPrefix string) (RaftStorage, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	s := &raftStorage{
		db:        db,
		path:      path,
		keyPrefix: keyPrefix,
	}
	return s, nil
}

func (s *raftStorage) Reset() {
	s.PutCommitIndex(0)
	s.PutCurrentTerm(0)
	s.PutLastApplied(0)
}

func (s *raftStorage) Close() {
	s.db.Close()
}

func (s *raftStorage) MustPutKV(key []byte, value []byte) {
	k := []byte(fmt.Sprintf("d:%s", key))
	err := s.db.Put(k, value, nil)
	if err != nil {
		panic(err)
	}
}

func (s *raftStorage) MustGetKV(key []byte) ([]byte, bool) {
	k := []byte(fmt.Sprintf("d:%s", key))
	buf, err := s.db.Get(k, nil)
	if err == lerrors.ErrNotFound {
		return []byte{}, false
	} else if err != nil {
		panic(err)
	}
	return buf, true
}

func (s *raftStorage) MustDeleteKV(key []byte) {
	k := []byte(fmt.Sprintf("d:%s", key))
	err := s.db.Delete(k, nil)
	if err != lerrors.ErrNotFound {
		panic(err)
	}
}

func (s *raftStorage) MustGetCurrentTerm() uint64 {
	term, err := s.dbGetUint64([]byte(kCurrentTerm))
	if err == lerrors.ErrNotFound {
		return 0
	} else if err != nil {
		panic(err)
	}
	return term
}

func (s *raftStorage) PutCurrentTerm(v uint64) error {
	return s.dbPutUint64([]byte(kCurrentTerm), v)
}

func (s *raftStorage) MustGetCommitIndex() uint64 {
	r, err := s.dbGetUint64([]byte(kCommitIndex))
	if err == lerrors.ErrNotFound {
		return 0
	} else if err != nil {
		panic(err)
	}
	return r
}

func (s *raftStorage) PutCommitIndex(v uint64) error {
	return s.dbPutUint64([]byte(kCommitIndex), v)
}

func (s *raftStorage) MustGetLastApplied() uint64 {
	r, err := s.dbGetUint64([]byte(kLastApplied))
	if err == lerrors.ErrNotFound {
		return 0
	} else if err != nil {
		panic(err)
	}
	return r
}

func (s *raftStorage) PutLastApplied(v uint64) error {
	return s.dbPutUint64([]byte(kLastApplied), v)
}

func (s *raftStorage) MustGetVotedFor() string {
	v, err := s.dbGetString([]byte(kVotedFor))
	if err == lerrors.ErrNotFound {
		return ""
	} else if err != nil {
		panic(err)
	}
	return v
}

func (s *raftStorage) PutVotedFor(v string) error {
	return s.dbPutString([]byte(kVotedFor), v)
}

func (s *raftStorage) AppendLogEntries(entries []RaftLogEntry) error {
	batch := new(leveldb.Batch)
	for _, le := range entries {
		k := makeLogEntryKey(le.Index)
		v, _ := json.Marshal(le)
		batch.Put(k, v)
	}
	err := s.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *raftStorage) AppendLogEntriesByCommands(commands []RaftCommand) (uint64, error) {
	lastIndex, _ := s.MustGetLastLogIndexAndTerm()
	term := s.MustGetCurrentTerm()
	es := []RaftLogEntry{}
	for _, cmd := range commands {
		le := RaftLogEntry{
			Index:   lastIndex + 1,
			Term:    term,
			Command: cmd,
		}
		lastIndex++
		es = append(es, le)
	}
	err := s.AppendLogEntries(es)
	return lastIndex, err
}

func (s *raftStorage) GetLogEntriesSince(index uint64) ([]RaftLogEntry, error) {
	rg := lutil.BytesPrefix([]byte(kLogEntries))
	rg.Start = makeLogEntryKey(index)
	iter := s.db.NewIterator(rg, nil)
	defer iter.Release()
	es := []RaftLogEntry{}
	for iter.Next() {
		buf := iter.Value()
		le := RaftLogEntry{}
		err := json.Unmarshal(buf, &le)
		if err != nil {
			return nil, err
		}
		es = append(es, le)
	}
	return es, nil
}

func (s *raftStorage) MustGetLogEntriesSince(index uint64) []RaftLogEntry {
	es, err := s.GetLogEntriesSince(index)
	if err != nil {
		panic(err)
	}
	return es
}

func (s *raftStorage) TruncateSince(index uint64) {
	entries := s.MustGetLogEntriesSince(index)
	batch := new(leveldb.Batch)
	for _, entry := range entries {
		batch.Delete(makeLogEntryKey(entry.Index))
	}
	err := s.db.Write(batch, nil)
	if err != nil {
		panic(err)
	}
}

func (s *raftStorage) getLastLogEntry() (*RaftLogEntry, error) {
	rg := lutil.BytesPrefix([]byte(kLogEntries))
	iter := s.db.NewIterator(rg, nil)
	defer iter.Release()
	exists := iter.Last()
	if !exists {
		return nil, nil
	}
	buf := iter.Value()
	le := RaftLogEntry{}
	err := json.Unmarshal(buf, &le)
	if err != nil {
		return nil, err
	}
	return &le, nil
}

func (s *raftStorage) MustGetLastLogIndexAndTerm() (uint64, uint64) {
	le, err := s.getLastLogEntry()
	if le == nil && err == nil {
		return 0, 0
	}
	if err != nil {
		panic(err)
	}
	return le.Index, le.Term
}

func (s *raftStorage) dbGetUint64(k []byte) (uint64, error) {
	key := []byte(fmt.Sprintf("%s:%s", s.keyPrefix, k))
	buf, err := s.db.Get(key, nil)
	if err != nil {
		return 0, err
	}
	n := binary.LittleEndian.Uint64(buf)
	return n, nil
}

func (s *raftStorage) dbPutUint64(k []byte, v uint64) error {
	key := []byte(fmt.Sprintf("%s:%s", s.keyPrefix, k))
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	return s.db.Put(key, buf, nil)
}

func (s *raftStorage) dbGetString(k []byte) (string, error) {
	key := []byte(fmt.Sprintf("%s:%s", s.keyPrefix, k))
	buf, err := s.db.Get(key, nil)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (s *raftStorage) dbPutString(k []byte, v string) error {
	key := []byte(fmt.Sprintf("%s:%s", s.keyPrefix, k))
	return s.db.Put(key, []byte(v), nil)
}

func makeLogEntryKey(index uint64) []byte {
	return []byte(fmt.Sprintf("%s:%s", kLogEntries, uint64ToBytes(index)))
}

// uint64ToBytes converts an uint64 number to a lexicographically order bytes
func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
