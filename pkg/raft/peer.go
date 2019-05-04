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

func (p *Peer) SendAppendEntriesRequest(request *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	url := fmt.Sprintf("http://%s/_raft/append-entries", p.Addr)
	resp := AppendEntriesResponse{}
	err := p.post(url, request, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (p *Peer) SendRequestVoteRequest(request *RequestVoteRequest) (*RequestVoteResponse, error) {
	url := fmt.Sprintf("http://%s/_raft/request-vote", p.Addr)
	resp := RequestVoteResponse{}
	err := p.post(url, request, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (p *Peer) post(url string, request interface{}, response interface{}) error {
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
	err = json.Unmarshal(body, response)
	if err != nil {
		return err
	}
	return nil
}
