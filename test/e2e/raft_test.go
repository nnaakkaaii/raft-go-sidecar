package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nnaakkaaii/raft-actor-model/pkg/log"
	"github.com/nnaakkaaii/raft-actor-model/pkg/raft"
	"github.com/nnaakkaaii/raft-actor-model/pkg/storage"
)

func TestRaft(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		wg := sync.WaitGroup{}

		peers := map[int32]*raft.Peer{
			0: raft.NewPeer(0, "localhost:50000"),
			1: raft.NewPeer(1, "localhost:50001"),
			2: raft.NewPeer(2, "localhost:50002"),
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			s := storage.NewFileStorage("raft_test_1_peer_0_storage.json")
			if err := s.Open(); err != nil {
				t.Error(err)
				return
			}
			defer s.Close()

			server := raft.NewServer(
				peers[0].ID,
				peers[0].Address,
				map[int32]*raft.Peer{
					1: peers[1],
					2: peers[2],
				},
				s,
				log.NewFileLogStorage("raft_test_1_peer_0_log.json", 1024),
			)
			commitCh := make(chan raft.Entry)
			server.Run(ctx, commitCh)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			s := storage.NewFileStorage("raft_test_1_peer_1_storage.json")
			if err := s.Open(); err != nil {
				t.Error(err)
				return
			}
			defer s.Close()

			server := raft.NewServer(
				peers[1].ID,
				peers[1].Address,
				map[int32]*raft.Peer{
					0: peers[0],
					2: peers[2],
				},
				s,
				log.NewFileLogStorage("raft_test_1_peer_1_log.json", 1024),
			)
			commitCh := make(chan raft.Entry)
			server.Run(ctx, commitCh)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			s := storage.NewFileStorage("raft_test_1_peer_2_storage.json")
			if err := s.Open(); err != nil {
				t.Error(err)
				return
			}
			defer s.Close()

			server := raft.NewServer(
				peers[2].ID,
				peers[2].Address,
				map[int32]*raft.Peer{
					0: peers[0],
					1: peers[1],
				},
				s,
				log.NewFileLogStorage("raft_test_1_peer_2_log.json", 1024),
			)
			commitCh := make(chan raft.Entry)
			server.Run(ctx, commitCh)
		}()

		time.Sleep(5 * time.Second)
		cancel()

		wg.Wait()
		return
	})
}
