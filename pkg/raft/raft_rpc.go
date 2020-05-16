package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Fleurer/miniraft/pkg/util"
)

type RaftRPC interface {
	RequestVote(p Peer, req *RequestVoteMessage) (*RequestVoteReply, error)
	AppendEntries(p Peer, req *AppendEntriesMessage) (*AppendEntriesReply, error)
}

type raftRPC struct {
	logger *util.Logger
}

func NewRaftRPC(logger *util.Logger) RaftRPC {
	return &raftRPC{logger: logger}
}

func (rr *raftRPC) AppendEntries(p Peer, request *AppendEntriesMessage) (*AppendEntriesReply, error) {
	url := fmt.Sprintf("http://%s/_raft/append-entries", p.Addr)
	resp := AppendEntriesReply{}
	err := rr.post(p, url, request, &resp)
	// rr.logger.Debugf("raftNode.request.send-append-entries to=%s msg=%#v err=%s", p.ID, request, err)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (rr *raftRPC) RequestVote(p Peer, request *RequestVoteMessage) (*RequestVoteReply, error) {
	url := fmt.Sprintf("http://%s/_raft/request-vote", p.Addr)
	resp := RequestVoteReply{}
	err := rr.post(p, url, request, &resp)
	rr.logger.Debugf("raftNode.request.send-request vote to=%s msg=%#v err=%s", p.ID, request, err)
	if err != nil {
		return nil, err
	}
	return &resp, nil
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

func (r *mockRaftRequester) RequestVote(p Peer, req *RequestVoteMessage) (*RequestVoteReply, error) {
	raft := r.rafts[p.ID]
	ev := newRaftEV(req)

	select {
	case <-raft.closed:
		return nil, errors.New("raft closed")
	default:
	}

	raft.eventc <- ev
	resp := <-ev.replyc
	if r, ok := resp.(*MessageReply); ok {
		return nil, fmt.Errorf("bad result: %s", r.Message)
	} else if r, ok := resp.(*RequestVoteReply); ok {
		return r, nil
	} else {
		return nil, fmt.Errorf("unknown type")
	}
}

func (r *mockRaftRequester) AppendEntries(p Peer, req *AppendEntriesMessage) (*AppendEntriesReply, error) {
	raft := r.rafts[p.ID]
	ev := newRaftEV(req)

	select {
	case <-raft.closed:
		return nil, errors.New("raft closed")
	default:
	}

	raft.eventc <- ev
	resp := <-ev.replyc

	if r, ok := resp.(*MessageReply); ok {
		return nil, fmt.Errorf("bad result: %s", r.Message)
	} else if r, ok := resp.(*AppendEntriesReply); ok {
		return r, nil
	} else {
		return nil, fmt.Errorf("unknown type")
	}
}
