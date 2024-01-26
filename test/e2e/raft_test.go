package e2e

import (
	"context"
	"fmt"
	workerv1 "github.com/nnaakkaaii/raft-actor-model/proto/worker/v1"
	"sync"
	"testing"
	"time"

	"github.com/nnaakkaaii/raft-actor-model/pkg/log"
	"github.com/nnaakkaaii/raft-actor-model/pkg/raft"
	"github.com/nnaakkaaii/raft-actor-model/pkg/storage"
)

func newServer(test int, id int32, num int32) (*raft.Server, func() error) {
	peers := map[int32]*raft.Peer{}
	for i := int32(0); i < num; i++ {
		if i == id {
			continue
		}
		peers[i] = raft.NewPeer(i, fmt.Sprintf("localhost:%d", 50000+i))
	}
	s := storage.NewFileStorage(fmt.Sprintf("raft_test_%d_peer_%d_storage.json", test, id))
	if err := s.Open(); err != nil {
		panic(err)
	}

	return raft.NewServer(
		id,
		fmt.Sprintf("localhost:%d", 50000+id),
		peers,
		s,
		log.NewFileLogStorage(fmt.Sprintf("raft_test_%d_peer_%d_log.json", test, id), 1024),
	), s.Close
}

func TestRaft(t *testing.T) {
	t.Run("test 0: start", func(t *testing.T) {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		wg := sync.WaitGroup{}

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(id int32) {
				defer wg.Done()

				server, cls := newServer(0, id, 3)
				defer cls()
				server.Run(ctx, make(chan raft.Entry))
			}(int32(i))
		}

		time.Sleep(5 * time.Second)
		cancel()

		wg.Wait()
		return
	})
	t.Run("test 1: shutdown leader", func(t *testing.T) {
		wg := sync.WaitGroup{}
		pctx, pcancel := context.WithCancel(context.Background())

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(id int32) {
				defer wg.Done()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				server, cls := newServer(2, id, 3)
				defer cls()
				go server.Run(ctx, make(chan raft.Entry))
				time.Sleep(3 * time.Second)

				if server.Role() == raft.Leader {
					cancel()
					time.Sleep(3 * time.Second)

					ctx, cancel = context.WithCancel(context.Background())
					defer cancel()
					server, cls = newServer(2, id, 3)
					defer cls()
					go server.Run(ctx, make(chan raft.Entry))
				}
				<-pctx.Done()
			}(int32(i))
		}

		time.Sleep(10 * time.Second)
		pcancel()

		wg.Wait()
		return
	})
	t.Run("test 2: log replication", func(t *testing.T) {
		wg := sync.WaitGroup{}
		pctx, pcancel := context.WithCancel(context.Background())

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(id int32) {
				defer wg.Done()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				server, cls := newServer(2, id, 3)
				defer cls()

				commitCh := make(chan raft.Entry)
				go server.Run(ctx, commitCh)
				go func() {
					for {
						select {
						case commit := <-commitCh:
							if commit.Command != "hello" {
								t.Errorf("inconsistent command on follower %d", id)
							}
							return
						case <-pctx.Done():
							t.Errorf("follower %d failed to receive command", id)
							return
						}
					}
				}()
				time.Sleep(3 * time.Second)

				if server.Role() == raft.Leader {
					resp, err := server.Submit(ctx, &workerv1.SubmitRequest{Command: "hello"})
					if err != nil {
						t.Error(err)
						return
					}
					if !resp.Success {
						t.Errorf("failed submitting command to leader %d", id)
					}
				}
				<-pctx.Done()
			}(int32(i))
		}

		time.Sleep(5 * time.Second)
		pcancel()

		wg.Wait()
		return
	})
}
