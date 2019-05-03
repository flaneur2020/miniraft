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

func NewRaftServer(r *Raft, listenAddr string) *RaftServer {
	s := &RaftServer{raft: r, listenAddr: listenAddr}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	s.httpServer = &http.Server{Addr: listenAddr, Handler: mux}
	return s
}

func (s *RaftServer) ListenAndServe() error {
	log.Printf("server start: listen=%s", s.listenAddr)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	return s.httpServer.ListenAndServe()
}

func (s *RaftServer) handleHealth(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "{\"health\": true}")
}

func (s *RaftServer) Shutdown() error {
	log.Printf("closing raft server: listen=%s", s.listenAddr)
	ctx := context.TODO()
	err := s.httpServer.Shutdown(ctx)
	return err
}
