package raft

import (
	"context"
	"fmt"
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
	s.httpServer = &http.Server{Addr: opt.ListenAddr, Handler: m}
	return s, nil
}

func (s *RaftServer) ListenAndServe() error {
	log.Printf("server start: listen=%s", s.listenAddr)
	go s.raft.Loop()
	return s.httpServer.ListenAndServe()
}

func (s *RaftServer) handleHealth(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "{\"health\": true}")
}

func (s *RaftServer) Shutdown() error {
	log.Printf("closing raft server: listen=%s", s.listenAddr)
	s.raft.Shutdown()
	ctx := context.TODO()
	return s.httpServer.Shutdown(ctx)
}
