package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type Peer struct {
	ID   string
	Addr string
}

func (p *Peer) SendAppendEntriesRequest(request *AppendEntriesRequest) (*RaftResponse, error) {
	url := fmt.Sprintf("http://%s/_raft/append-entries", p.Addr)
	return p.post(url, request)
}

func (p *Peer) SendRequestVoteRequest(request *RequestVoteRequest) (*RaftResponse, error) {
	url := fmt.Sprintf("http://%s/_raft/request-vote", p.Addr)
	return p.post(url, request)
}

func (p *Peer) post(url string, request interface{}) (*RaftResponse, error) {
	buf, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	c := http.Client{Timeout: time.Duration(100 * time.Millisecond)}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	rresp := RaftResponse{}
	err = json.Unmarshal(body, &rresp)
	if err != nil {
		return nil, err
	}
	return &rresp, nil
}
