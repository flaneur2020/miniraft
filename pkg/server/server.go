package server

import (
	"context"
	"encoding/json"
	"github.com/Fleurer/miniraft/pkg/raft"
	"io/ioutil"
	"net/http"
)

type RaftServer struct {
	r          raft.Raft
	listenAddr string
	httpServer *http.Server
}

func NewRaftServer(opt *raft.RaftOptions) (*RaftServer, error) {
	r, err := raft.NewRaft(opt)
	if err != nil {
		return nil, err
	}
	s := &RaftServer{r: r, listenAddr: opt.ListenAddr}
	m := http.NewServeMux()
	m.HandleFunc("/health", s.handleHealth)
	m.HandleFunc("/_raft/append-entries", s.handleAppendEntries)
	m.HandleFunc("/_raft/request-vote", s.handleRequestVote)
	m.HandleFunc("/_raft/command", s.handleCommand)
	m.HandleFunc("/_raft/status", s.handleStatus)
	s.httpServer = &http.Server{Addr: opt.ListenAddr, Handler: m}
	return s, nil
}

func (s *RaftServer) ListenAndServe() error {
	go s.r.Loop()
	return s.httpServer.ListenAndServe()
}

func (s *RaftServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.response(w, raft.ServerReply{Code: raft.SUCCESS, Message: "health"})
}

func (s *RaftServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.response(w, raft.ServerReply{Code: raft.SUCCESS, Message: "health"})
}

func (s *RaftServer) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	msg := raft.AppendEntriesMessage{}
	err := s.parseRequest(r, &msg)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}

	reply, err := s.r.Process(&msg)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}

	s.response(w, reply)
}

func (s *RaftServer) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	msg := raft.RequestVoteMessage{}
	err := s.parseRequest(r, &msg)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}

	reply, err := s.r.Process(&msg)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}

	s.response(w, reply)
}

func (s *RaftServer) handleCommand(w http.ResponseWriter, r *http.Request) {
	msg := raft.CommandMessage{}
	err := s.parseRequest(r, &msg)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}

	reply, err := s.r.Process(&msg)
	if err != nil {
		s.responseError(w, 400, err.Error())
		return
	}

	s.response(w, reply)
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

func (s *RaftServer) response(w http.ResponseWriter, resp interface{}) {
	buf, err := json.Marshal(resp)
	if err != nil {
		s.responseError(w, 500, err.Error())
	}
	w.WriteHeader(200)
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
	s.r.Shutdown()
	ctx := context.TODO()
	return s.httpServer.Shutdown(ctx)
}
