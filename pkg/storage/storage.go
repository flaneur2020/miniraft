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
	kMeta       = "m:meta"
	kLogEntries = "l:log-entries"
)

type RaftLogEntry struct {
	Term    uint64      `json:"term"`
	Index   uint64      `json:"index"`
	Command RaftCommand `json:"command"`
}

type RaftCommand struct {
	Type  string `json:"opType"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

const (
	NopCommandType    = "nop"
	PutCommandType    = "put"
	GetCommandType    = "get"
	DeleteCommandType = "delete"
)

var (
	NopCommand = RaftCommand{Type: NopCommandType, Key: []byte{}, Value: []byte{}}
)

type RaftHardState struct {
	LastApplied uint64 `json:"lastApplied"`
	VotedFor    string `json:"votedFor"`
	CurrentTerm uint64 `json:"currentTerm"`
}

type RaftStorage interface {
	MustGetKV([]byte) ([]byte, bool)
	MustPutKV([]byte, []byte)
	MustDeleteKV([]byte)

	GetHardState() (*RaftHardState, error)
	PutHardState(s *RaftHardState) error

	MustGetLastLogIndexAndTerm() (uint64, uint64)
	MustGetLogEntriesSince(index uint64) []RaftLogEntry
	AppendBulkLogEntries(entries []RaftLogEntry) error
	AppendLogEntry(command RaftCommand, term uint64) (uint64, error)
	TruncateSince(index uint64) (int, error)

	Reset() error
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

func (rs *raftStorage) Reset() error {
	hs := &RaftHardState{
		LastApplied: 0,
		VotedFor:    "",
		CurrentTerm: 0,
	}

	if err := rs.PutHardState(hs); err != nil {
		return err
	}

	if _, err := rs.TruncateSince(0); err != nil {
		return err
	}

	return nil
}

func (rs *raftStorage) PutHardState(m *RaftHardState) error {
	buf, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return rs.dbPutString([]byte(kMeta), string(buf))
}

func (rs *raftStorage) GetHardState() (*RaftHardState, error) {
	var (
		m = RaftHardState{
			VotedFor:    "",
			LastApplied: 0,
			CurrentTerm: 0,
		}
		err error
		s   string
	)

	s, err = rs.dbGetString([]byte(kMeta))
	if err == lerrors.ErrNotFound {
		return &m, nil
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(s), &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (rs *raftStorage) Close() {
	rs.db.Close()
}

func (rs *raftStorage) MustPutKV(key []byte, value []byte) {
	k := []byte(fmt.Sprintf("d:%rs", key))
	err := rs.db.Put(k, value, nil)
	if err != nil {
		panic(err)
	}
}

func (rs *raftStorage) MustGetKV(key []byte) ([]byte, bool) {
	k := []byte(fmt.Sprintf("d:%rs", key))
	buf, err := rs.db.Get(k, nil)
	if err == lerrors.ErrNotFound {
		return []byte{}, false
	} else if err != nil {
		panic(err)
	}
	return buf, true
}

func (rs *raftStorage) MustDeleteKV(key []byte) {
	k := []byte(fmt.Sprintf("d:%rs", key))
	err := rs.db.Delete(k, nil)
	if err != lerrors.ErrNotFound {
		panic(err)
	}
}

func (rs *raftStorage) AppendBulkLogEntries(entries []RaftLogEntry) error {
	batch := new(leveldb.Batch)
	for _, le := range entries {
		k := makeLogEntryKey(le.Index)
		v, _ := json.Marshal(le)
		batch.Put(k, v)
	}
	err := rs.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (rs *raftStorage) AppendLogEntry(command RaftCommand, term uint64) (uint64, error) {
	lastIndex, _ := rs.MustGetLastLogIndexAndTerm()
	entry := RaftLogEntry{
		Index:   lastIndex + 1,
		Term:    term,
		Command: command,
	}
	lastIndex++
	err := rs.AppendBulkLogEntries([]RaftLogEntry{entry})
	return lastIndex, err
}

func (rs *raftStorage) GetLogEntriesSince(index uint64) ([]RaftLogEntry, error) {
	rg := lutil.BytesPrefix([]byte(kLogEntries))
	rg.Start = makeLogEntryKey(index)
	iter := rs.db.NewIterator(rg, nil)
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

func (rs *raftStorage) MustGetLogEntriesSince(index uint64) []RaftLogEntry {
	es, err := rs.GetLogEntriesSince(index)
	if err != nil {
		panic(err)
	}
	return es
}

func (rs *raftStorage) TruncateSince(index uint64) (int, error) {
	entries := rs.MustGetLogEntriesSince(index)
	batch := new(leveldb.Batch)
	for _, entry := range entries {
		batch.Delete(makeLogEntryKey(entry.Index))
	}

	err := rs.db.Write(batch, nil)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func (rs *raftStorage) getLastLogEntry() (*RaftLogEntry, error) {
	rg := lutil.BytesPrefix([]byte(kLogEntries))
	iter := rs.db.NewIterator(rg, nil)
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

func (rs *raftStorage) MustGetLastLogIndexAndTerm() (uint64, uint64) {
	le, err := rs.getLastLogEntry()
	if le == nil && err == nil {
		return 0, 0
	}
	if err != nil {
		panic(err)
	}
	return le.Index, le.Term
}

func (rs *raftStorage) dbGetString(k []byte) (string, error) {
	key := []byte(fmt.Sprintf("%rs:%rs", rs.keyPrefix, k))
	buf, err := rs.db.Get(key, nil)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (rs *raftStorage) dbPutString(k []byte, v string) error {
	key := []byte(fmt.Sprintf("%rs:%rs", rs.keyPrefix, k))
	return rs.db.Put(key, []byte(v), nil)
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
