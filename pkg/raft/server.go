package raft

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type RaftServer struct {
	raft       *Raft
	listenAddr string
	httpServer *http.Server
}

func NewRaftServer(opt *RaftOptions) (*RaftServer, error) {
	r, err := NewRaft(opt)
	if err != nil {
		return nil, err
	}
	s := &RaftServer{raft: r, listenAddr: opt.ListenAddr}
	m := http.NewServeMux()
	m.HandleFunc("/health", s.handleHealth)
	m.HandleFunc("/_raft/append-entries", s.handleAppendEntries)
	m.HandleFunc("/_raft/request-vote", s.handleRequestVote)
	m.HandleFunc("/_raft/status", s.handleStatus)
	s.httpServer = &http.Server{Addr: opt.ListenAddr, Handler: m}
	return s, nil
}

func (s *RaftServer) ListenAndServe() error {
	log.Printf("server start: listen=%s", s.listenAddr)
	go s.raft.Loop()
	return s.httpServer.ListenAndServe()
}

func (s *RaftServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.response(w, RaftResponse{Code: SUCCESS, Message: "health"})
}

func (s *RaftServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.response(w, RaftResponse{Code: SUCCESS, Message: "health"})
}

func (s *RaftServer) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	req := AppendEntriesRequest{}
	err := s.parseRequest(r, &req)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}
	s.raft.reqc <- req
	resp := <-s.raft.respc
	s.response(w, resp)
}

func (s *RaftServer) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	req := RequestVoteRequest{}
	err := s.parseRequest(r, &req)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}
	s.raft.reqc <- req
	resp := <-s.raft.respc
	s.response(w, resp)
}

func (s *RaftServer) parseRequest(r *http.Request, target interface{}) error {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	err = json.Unmarshal(buf, target)
	if err != nil {
		return err
	}
	return nil
}

func (s *RaftServer) response(w http.ResponseWriter, resp RaftResponse) {
	buf, err := json.Marshal(resp)
	if err != nil {
		s.responseError(w, 500, err.Error())
	}
	w.WriteHeader(resp.Code)
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

func (s *RaftServer) responseError(w http.ResponseWriter, code int, message string) {
	resp := map[string]interface{}{"code": code, "message": message}
	buf, _ := json.Marshal(resp)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

func (s *RaftServer) Shutdown() error {
	log.Printf("closing raft server: listen=%s", s.listenAddr)
	s.raft.Shutdown()
	ctx := context.TODO()
	return s.httpServer.Shutdown(ctx)
}
