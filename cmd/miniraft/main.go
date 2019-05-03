package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Fleurer/miniraft/pkg/raft"
)

func parseOpt(path string) (*raft.RaftOptions, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config file failed: %s", err)
	}
	opt := raft.RaftOptions{}
	dec := json.NewDecoder(file)
	err = dec.Decode(&opt)
	if err != nil {
		return nil, fmt.Errorf("decode config file failed: %s", err)
	}
	return &opt, nil
}

func run(path string) {
	opt, err := parseOpt(path)
	if err != nil {
		panic(fmt.Sprintf("load raft option failed: %s", err))
	}

	r, err := raft.NewRaft(opt)
	if err != nil {
		panic(fmt.Sprintf("new raft failed: %s", err))
	}
	defer r.Close()
	go r.Loop()

	rs := raft.NewRaftServer(r, opt.ListenAddr)

	go func() {
		sc := make(chan os.Signal, 1)
		signal.Notify(sc,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT)
		<-sc
		if err := rs.Shutdown(); err != nil {
			panic(fmt.Sprintf("fail on shutdown: %s", err))
		}
	}()

	err = rs.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		panic(fmt.Sprintf("fail on "))
	}
}

func help() {
	fmt.Printf("bin/miniraft -conf <path>\n")
}

func main() {
	var conf string
	flag.StringVar(&conf, "conf", "", "config")
	flag.Parse()

	if len(os.Args) <= 1 {
		help()
	} else {
		run(conf)
	}
}
