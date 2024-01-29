package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	raft2 "github.com/nnaakkaaii/raft-go-sidecar"
)

func main() {
	var (
		idStr string
	)
	flag.StringVar(&idStr, "id", "", "The ID to use (int32)")

	flag.Parse()

	tid, err := strconv.ParseInt(idStr, 10, 32)
	if err != nil {
		panic(err)
	}

	id := int32(tid)
	s := raft2.NewFileStorage(fmt.Sprintf("tmp/raft_peer_%d_storage.json", id))
	if err := s.Open(); err != nil {
		panic(err)
	}
	defer s.Close()
	l, err := raft2.NewLevelDBLogStorage(fmt.Sprintf("tmp/raft_peer_%d_log.db", id), 1000)
	if err != nil {
		panic(err)
	}
	defer l.Close()

	svr := raft2.NewServer(
		id,
		fmt.Sprintf("localhost:%d", 50000+id),
		map[int32]*raft2.Peer{},
		s,
		l,
	)

	ctx := context.Background()
	commitCh := make(chan raft2.Entry)
	go svr.Run(ctx, commitCh)

	for {
		select {
		case commit := <-commitCh:
			fmt.Printf("%v [%d] received commit %+v\n", time.Now(), id, commit)
		case <-ctx.Done():
			fmt.Printf("%v [%d] %+v\n", time.Now(), id, ctx.Err())
			return
		}
	}
}
