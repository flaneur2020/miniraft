package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Fleurer/miniraft/pkg/data"
	"io/ioutil"
	"net/http"
	"time"
)

type RaftRequester interface {
	SendRequestVoteRequest(p Peer, req *data.RequestVoteRequest) (*data.RequestVoteResponse, error)
	SendAppendEntriesRequest(p Peer, req *data.AppendEntriesRequest) (*data.AppendEntriesResponse, error)
}

type raftRequester struct {
	logger *Logger
}

func NewRaftRequester(logger *Logger) RaftRequester {
	return &raftRequester{logger: logger}
}

func (rr *raftRequester) SendAppendEntriesRequest(p Peer, request *data.AppendEntriesRequest) (*data.AppendEntriesResponse, error) {
	url := fmt.Sprintf("http://%s/_raft/append-entries", p.Addr)
	resp := data.AppendEntriesResponse{}
	err := rr.post(p, url, request, &resp)
	// rr.logger.Debugf("raft.request.send-append-entries to=%s req=%#v err=%s", p.ID, request, err)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (rr *raftRequester) SendRequestVoteRequest(p Peer, request *data.RequestVoteRequest) (*data.RequestVoteResponse, error) {
	url := fmt.Sprintf("http://%s/_raft/request-vote", p.Addr)
	resp := data.RequestVoteResponse{}
	err := rr.post(p, url, request, &resp)
	rr.logger.Debugf("raft.request.send-request vote to=%s req=%#v err=%s", p.ID, request, err)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (rr *raftRequester) post(p Peer, url string, request interface{}, response interface{}) error {
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
	rafts map[string]*raft
}

func (r *mockRaftRequester) SendRequestVoteRequest(p Peer, req *data.RequestVoteRequest) (*data.RequestVoteResponse, error) {
	raft := r.rafts[p.ID]
	ev := newRaftEV(req)
	raft.eventc <- ev
	resp := (<-ev.respc).(data.RequestVoteResponse)
	return &resp, nil
}

func (r *mockRaftRequester) SendAppendEntriesRequest(p Peer, req *data.AppendEntriesRequest) (*data.AppendEntriesResponse, error) {
	raft := r.rafts[p.ID]
	ev := newRaftEV(req)
	raft.eventc <- ev
	resp := (<-ev.respc).(data.AppendEntriesResponse)
	return &resp, nil
}
