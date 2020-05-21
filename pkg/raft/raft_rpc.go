package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Fleurer/miniraft/pkg/util"
)

type RaftRPC interface {
	SendRequestVote(p Peer, req *RequestVoteMsg, cb func(RaftMessage))
	SendAppendEntries(p Peer, req *AppendEntriesMsg, cb func(RaftMessage))
}

type raftRPC struct {
	logger *util.Logger
}

func NewRaftRPC(logger *util.Logger) RaftRPC {
	return &raftRPC{logger: logger}
}

func (rr *raftRPC) SendAppendEntries(p Peer, request *AppendEntriesMsg, cb func(msg RaftMessage)) {
	url := fmt.Sprintf("http://%s/_raft/append-entries", p.Addr)
	msg := AppendEntriesReplyMsg{}
	err := rr.post(p, url, request, &msg)
	// rr.logger.Debugf("raftNode.request.send-append-entries to=%s msg=%#v err=%s", p.ID, request, err)
	if err != nil {
		return
	}

	cb(&msg)
}

func (rr *raftRPC) SendRequestVote(p Peer, request *RequestVoteMsg, cb func(msg RaftMessage)) {
	url := fmt.Sprintf("http://%s/_raft/request-vote", p.Addr)
	msg := RequestVoteReplyMsg{}
	err := rr.post(p, url, request, &msg)
	// rr.logger.Debugf("raftNode.request.send-request vote to=%s msg=%#v err=%s", p.ID, request, err)
	if err != nil {
		return
	}

	cb(&msg)
}

func (rr *raftRPC) post(p Peer, url string, request interface{}, response interface{}) error {
	buf, err := json.Marshal(request)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return err
	}

	c := http.Client{Timeout: time.Duration(100 * time.Millisecond)}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, response)
}

type mockRaftRequester struct {
	rafts map[string]*raftNode
}

func (rq *mockRaftRequester) SendRequestVote(p Peer, req *RequestVoteMsg, cb func(RaftMessage)) {
	raft := rq.rafts[p.ID]
	ev := newRaftEV(req)

	select {
	case <-raft.closed:
		return
	default:
	}

	raft.eventc <- ev
	<-ev.c
	msg := ev.reply
	if msg, ok := msg.(*RequestVoteReplyMsg); ok {
		cb(msg)
	}
}

func (rq *mockRaftRequester) SendAppendEntries(p Peer, req *AppendEntriesMsg, cb func(RaftMessage)) {
	raft := rq.rafts[p.ID]
	ev := newRaftEV(req)

	select {
	case <-raft.closed:
		return
	default:
	}

	raft.eventc <- ev
	<-ev.c
	msg := ev.reply

	if msg, ok := msg.(*AppendEntriesReplyMsg); ok {
		cb(msg)
	}
}
