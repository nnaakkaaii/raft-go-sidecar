package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/nnaakkaaii/raft-gochannel/pkg/log"
)

func main() {
	var (
		path string
	)
	flag.StringVar(&path, "path", "", "path")

	flag.Parse()

	logs, err := log.NewLevelDBLogStorage(path, 1024)
	if err != nil {
		panic(err)
	}
	defer logs.Close()
	for _, l := range logs.Slice(context.Background(), 0) {
		fmt.Printf("%d,%d,%s\n", l.Term, l.Index, l.Command)
	}
}
