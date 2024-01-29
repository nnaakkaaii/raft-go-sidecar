package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/nnaakkaaii/raft-go-sidecar"
)

func main() {
	var (
		path string
	)
	flag.StringVar(&path, "path", "", "path")

	flag.Parse()

	logs, err := raft.NewLevelDBLogStorage(path, 1024)
	if err != nil {
		panic(err)
	}
	defer logs.Close()
	for _, l := range logs.Slice(context.Background(), 0) {
		fmt.Printf("%d,%d,%s\n", l.Term, l.Index, l.Command)
	}
}
