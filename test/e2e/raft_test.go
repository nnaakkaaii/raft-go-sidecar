package e2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"

	"github.com/nnaakkaaii/raft-gochannel/pkg/log"
	"github.com/nnaakkaaii/raft-gochannel/pkg/raft"
	"github.com/nnaakkaaii/raft-gochannel/pkg/storage"
	"github.com/nnaakkaaii/raft-gochannel/proto/peer/v1"
)

func tmp(test int, num int) (map[int]string, map[int]string, func()) {
	var cleans []func()
	files := map[int]string{}
	for id := 0; id < num; id++ {
		file, err := os.CreateTemp("", fmt.Sprintf("raft_test_%d_peer_%d_storage.json", test, id))
		if err != nil {
			panic(err)
		}
		files[id] = file.Name()
		cleans = append(cleans, func() { os.RemoveAll(file.Name()) })
	}
	dirs := map[int]string{}
	for id := 0; id < num; id++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("raft_test_%d_peer_%d_log", test, id))
		if err != nil {
			panic(err)
		}
		dirs[id] = dir
		cleans = append(cleans, func() { os.RemoveAll(dir) })
	}
	return files, dirs, func() {
		for _, c := range cleans {
			c()
		}
	}
}

func newServer(id, num int32, file, dir string, cacheSize *int) (*raft.Server, raft.Log, func()) {
	cache := 1000
	if cacheSize != nil {
		cache = *cacheSize
	}

	peers := map[int32]*raft.Peer{}
	for i := int32(0); i < num; i++ {
		if i == id {
			continue
		}
		peers[i] = raft.NewPeer(i, fmt.Sprintf("localhost:%d", 50000+i))
	}

	s := storage.NewFileStorage(file)
	if err := s.Open(); err != nil {
		panic(err)
	}

	l, err := log.NewLevelDBLogStorage(dir, cache)
	if err != nil {
		panic(fmt.Sprintf("%d: %+v", id, err))
	}

	cls := func() {
		if err := s.Close(); err != nil {
			panic(err)
		}
		if err := l.Close(); err != nil {
			panic(err)
		}
	}

	return raft.NewServer(
		id,
		fmt.Sprintf("localhost:%d", 50000+id),
		peers,
		s,
		l,
	), l, cls
}

func TestRaft(t *testing.T) {
	t.Run("test 0: start", func(t *testing.T) {
		const test = 0
		const peer = 3

		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		wg := sync.WaitGroup{}
		files, dirs, clean := tmp(test, peer)
		defer clean()

		for i := 0; i < peer; i++ {
			wg.Add(1)
			go func(id int32, file, dir string) {
				defer wg.Done()

				server, _, cls := newServer(id, peer, file, dir, nil)
				defer cls()
				server.Run(ctx, make(chan raft.Entry))
			}(int32(i), files[i], dirs[i])
		}

		time.Sleep(5 * time.Second)
		cancel()

		wg.Wait()

		return
	})
	t.Run("test 1: shutdown leader", func(t *testing.T) {
		const test = 1
		const peer = 3

		wg := sync.WaitGroup{}
		pctx, pcancel := context.WithCancel(context.Background())
		files, dirs, clean := tmp(test, peer)
		defer clean()

		for i := 0; i < peer; i++ {
			wg.Add(1)
			go func(id int32, file, dir string) {
				defer wg.Done()

				ctx, cancel := context.WithCancel(context.Background())

				server, _, cls := newServer(id, peer, file, dir, nil)
				go server.Run(ctx, make(chan raft.Entry))
				time.Sleep(3 * time.Second)

				if server.Role() == raft.Leader {
					cancel()
					cls()
					time.Sleep(3 * time.Second)

					ctx, cancel = context.WithCancel(context.Background())
					server, _, cls = newServer(id, peer, file, dir, nil)
					go server.Run(ctx, make(chan raft.Entry))
				}
				<-pctx.Done()
				cancel()
				cls()
			}(int32(i), files[i], dirs[i])
		}

		time.Sleep(10 * time.Second)
		pcancel()

		wg.Wait()

		return
	})
	t.Run("test 2: log replication", func(t *testing.T) {
		const test = 2
		const peer = 3

		wg := sync.WaitGroup{}
		pctx, pcancel := context.WithCancel(context.Background())
		files, dirs, clean := tmp(test, peer)
		defer clean()

		for i := 0; i < peer; i++ {
			wg.Add(1)
			go func(id int32, file, dir string) {
				defer wg.Done()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				server, _, cls := newServer(id, peer, file, dir, nil)
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
					resp, err := server.Submit(ctx, &peerv1.SubmitRequest{Command: "hello"})
					if err != nil {
						t.Error(err)
						return
					}
					if !resp.Success {
						t.Errorf("failed submitting command to leader %d", id)
					}
				}
				<-pctx.Done()
			}(int32(i), files[i], dirs[i])
		}

		time.Sleep(5 * time.Second)
		pcancel()

		wg.Wait()

		return
	})
	t.Run("test 3: monkey testing", func(t *testing.T) {
		const test = 3
		const peer = 5

		wg := sync.WaitGroup{}
		pctx, pcancel := context.WithCancel(context.Background())

		files, dirs, clean := tmp(test, peer)
		defer clean()

		commandchs := map[int32]chan string{}
		cancelchs := map[int32]chan bool{}
		peerStatus := make(map[int32]bool)
		peerStatusMu := sync.RWMutex{}
		bestable := make(chan bool)

		final := map[int32][]raft.Entry{}
		finalMu := sync.Mutex{}

		for i := 0; i < peer; i++ {
			wg.Add(1)
			commandchs[int32(i)] = make(chan string)
			cancelchs[int32(i)] = make(chan bool)
			go func(id int32, file, dir string) {
				defer wg.Done()

				ctx, cancel := context.WithCancel(context.Background())
				server, l, cls := newServer(id, peer, file, dir, nil)

				commitCh := make(chan raft.Entry)
				go func() {
					err := server.Run(ctx, commitCh)
					if err != nil && !errors.Is(err, context.Canceled) {
						panic(err)
					}
				}()
				fmt.Printf("peer %d started!\n", id)

				peerStatusMu.Lock()
				peerStatus[id] = true
				peerStatusMu.Unlock()

				for {
					select {
					case <-commitCh:
					case command := <-commandchs[id]:
						go func() {
							peerStatusMu.RLock()
							if !peerStatus[id] {
								peerStatusMu.RUnlock()
								return
							}
							peerStatusMu.RUnlock()
							if server.Role() == raft.Leader {
								if res, err := server.Submit(context.Background(), &peerv1.SubmitRequest{Command: command}); err != nil {
									fmt.Printf("[%d] %+v", id, err)
								} else if !res.Success {
									t.Errorf("[%d] submit failed", id)
								}
							}
						}()
					case <-cancelchs[id]:
						go func() {
							cancel()
							cls()
							fmt.Printf("peer %d stopped!\n", id)
							peerStatusMu.Lock()
							peerStatus[id] = false
							peerStatusMu.Unlock()
							time.Sleep(2 * time.Second)

							ctx, cancel = context.WithCancel(context.Background())
							server, l, cls = newServer(id, peer, file, dir, nil)

							go func() {
								err := server.Run(ctx, commitCh)
								if err != nil && !errors.Is(err, context.Canceled) {
									panic(err)
								}
							}()
							fmt.Printf("peer %d started!\n", id)

							peerStatusMu.Lock()
							peerStatus[id] = true
							peerStatusMu.Unlock()
						}()
					case <-pctx.Done():
						finalMu.Lock()
						final[id] = l.Slice(context.TODO(), 0)
						finalMu.Unlock()
						cancel()
						cls()
						return
					}
				}
			}(int32(i), files[i], dirs[i])
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(5 * time.Second)
			ticker := time.NewTicker(100 * time.Millisecond)
			shouldStable := false

			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					go func() {
						if rand.Intn(5) == 0 {
							// submit
							fmt.Printf("command sending!\n")
							cmd := uuid.NewString()
							for i := 0; i < peer; i++ {
								commandchs[int32(i)] <- cmd
							}
							fmt.Printf("command sent!\n")
						}
						if rand.Intn(20) == 0 {
							if shouldStable {
								return
							}
							target := int32(rand.Intn(peer))
							fmt.Printf("peer %d downing!\n", target)
							peerStatusMu.RLock()
							if !peerStatus[target] {
								peerStatusMu.RUnlock()
								fmt.Printf("peer %d is already down!\n", target)
								return
							}
							downCount := 0
							for _, b := range peerStatus {
								if !b {
									downCount++
								}
							}
							peerStatusMu.RUnlock()
							if downCount >= 2 {
								fmt.Printf("peer %d cannot be down due to downcount %d!\n", target, downCount)
								return
							}
							cancelchs[target] <- true
							fmt.Printf("peer %d down!\n", target)
						}
					}()
				case <-bestable:
					shouldStable = true
				case <-pctx.Done():
					return
				}
			}
		}()

		time.Sleep(1 * time.Minute)
		bestable <- true
		time.Sleep(5 * time.Second)

		pcancel()

		wg.Wait()

		// 初めのキーとその値を取得
		var firstKey int32
		var firstValue []raft.Entry
		for key, value := range final {
			firstKey = key
			firstValue = value
			break
		}

		// 全てのエントリを最初のエントリと比較
		for key, value := range final {
			if key == firstKey {
				continue // 最初のエントリは比較しない
			}

			if diff := cmp.Diff(firstValue, value); diff != "" {
				t.Error(diff)
			}
		}

		return
	})
	t.Run("test 4: monkey testing without cache", func(t *testing.T) {
		const test = 3
		const peer = 5
		var cacheSize = 5

		wg := sync.WaitGroup{}
		pctx, pcancel := context.WithCancel(context.Background())

		files, dirs, clean := tmp(test, peer)
		defer clean()

		commandchs := map[int32]chan string{}
		cancelchs := map[int32]chan bool{}
		peerStatus := make(map[int32]bool)
		peerStatusMu := sync.RWMutex{}
		bestable := make(chan bool)

		final := map[int32][]raft.Entry{}
		finalMu := sync.Mutex{}

		for i := 0; i < peer; i++ {
			wg.Add(1)
			commandchs[int32(i)] = make(chan string)
			cancelchs[int32(i)] = make(chan bool)
			go func(id int32, file, dir string) {
				defer wg.Done()

				ctx, cancel := context.WithCancel(context.Background())
				server, l, cls := newServer(id, peer, file, dir, &cacheSize)

				commitCh := make(chan raft.Entry)
				go func() {
					err := server.Run(ctx, commitCh)
					if err != nil && !errors.Is(err, context.Canceled) {
						panic(err)
					}
				}()
				fmt.Printf("peer %d started!\n", id)

				peerStatusMu.Lock()
				peerStatus[id] = true
				peerStatusMu.Unlock()

				for {
					select {
					case <-commitCh:
					case command := <-commandchs[id]:
						go func() {
							peerStatusMu.RLock()
							if !peerStatus[id] {
								peerStatusMu.RUnlock()
								return
							}
							peerStatusMu.RUnlock()
							if server.Role() == raft.Leader {
								if res, err := server.Submit(context.Background(), &peerv1.SubmitRequest{Command: command}); err != nil {
									t.Error(err)
								} else if !res.Success {
									t.Errorf("[%d] submit failed", id)
								}
							}
						}()
					case <-cancelchs[id]:
						go func() {
							cancel()
							cls()
							fmt.Printf("peer %d stopped!\n", id)
							peerStatusMu.Lock()
							peerStatus[id] = false
							peerStatusMu.Unlock()
							time.Sleep(2 * time.Second)

							ctx, cancel = context.WithCancel(context.Background())
							server, l, cls = newServer(id, peer, file, dir, &cacheSize)

							go func() {
								err := server.Run(ctx, commitCh)
								if err != nil && !errors.Is(err, context.Canceled) {
									panic(err)
								}
							}()
							fmt.Printf("peer %d started!\n", id)

							peerStatusMu.Lock()
							peerStatus[id] = true
							peerStatusMu.Unlock()
						}()
					case <-pctx.Done():
						finalMu.Lock()
						final[id] = l.Slice(context.TODO(), 0)
						finalMu.Unlock()
						cancel()
						cls()
						return
					}
				}
			}(int32(i), files[i], dirs[i])
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(5 * time.Second)
			ticker := time.NewTicker(100 * time.Millisecond)
			shouldStable := false

			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					go func() {
						if rand.Intn(5) == 0 {
							// submit
							fmt.Printf("command sending!\n")
							cmd := uuid.NewString()
							for i := 0; i < peer; i++ {
								commandchs[int32(i)] <- cmd
							}
							fmt.Printf("command sent!\n")
						}
						if rand.Intn(20) == 0 {
							if shouldStable {
								return
							}
							target := int32(rand.Intn(peer))
							fmt.Printf("peer %d downing!\n", target)
							peerStatusMu.RLock()
							if !peerStatus[target] {
								peerStatusMu.RUnlock()
								fmt.Printf("peer %d is already down!\n", target)
								return
							}
							downCount := 0
							for _, b := range peerStatus {
								if !b {
									downCount++
								}
							}
							peerStatusMu.RUnlock()
							if downCount >= 2 {
								fmt.Printf("peer %d cannot be down due to downcount %d!\n", target, downCount)
								return
							}
							cancelchs[target] <- true
							fmt.Printf("peer %d down!\n", target)
						}
					}()
				case <-bestable:
					shouldStable = true
				case <-pctx.Done():
					return
				}
			}
		}()

		time.Sleep(1 * time.Minute)
		bestable <- true
		time.Sleep(5 * time.Second)

		pcancel()

		wg.Wait()

		// 初めのキーとその値を取得
		var firstKey int32
		var firstValue []raft.Entry
		for key, value := range final {
			firstKey = key
			firstValue = value
			break
		}

		// 全てのエントリを最初のエントリと比較
		for key, value := range final {
			if key == firstKey {
				continue // 最初のエントリは比較しない
			}

			if diff := cmp.Diff(firstValue, value); diff != "" {
				t.Error(diff)
			}
		}

		return
	})
	t.Run("test 5: monkey testing while changing config", func(t *testing.T) {
		const test = 5
		const peer = 3
		var cacheSize = 5

		wg := sync.WaitGroup{}
		pctx, pcancel := context.WithCancel(context.Background())

		files, dirs, clean := tmp(test, peer+2)
		defer clean()

		commandchs := map[int32]chan string{}
		configchs := map[int32]chan *peerv1.Configuration{}
		shutdownchs := map[int32]chan bool{}

		final := map[int32][]raft.Entry{}
		finalMu := sync.Mutex{}

		for i := 0; i < peer; i++ {
			wg.Add(1)
			commandchs[int32(i)] = make(chan string)
			configchs[int32(i)] = make(chan *peerv1.Configuration)
			shutdownchs[int32(i)] = make(chan bool)
			go func(id int32, file, dir string) {
				defer wg.Done()

				ctx, cancel := context.WithCancel(context.Background())
				server, l, cls := newServer(id, peer, file, dir, &cacheSize)

				commitCh := make(chan raft.Entry)
				go func() {
					err := server.Run(ctx, commitCh)
					if err != nil && !errors.Is(err, context.Canceled) {
						panic(err)
					}
				}()
				fmt.Printf("peer %d started!\n", id)

				for {
					select {
					case <-commitCh:
					case command := <-commandchs[id]:
						go func() {
							if server.Role() == raft.Leader {
								if res, err := server.Submit(context.Background(), &peerv1.SubmitRequest{Command: command}); err != nil {
									t.Error(err)
								} else if !res.Success {
									t.Errorf("[%d] submit failed", id)
								}
							}
						}()
					case config := <-configchs[id]:
						go func() {
							if server.Role() == raft.Leader {
								if res, err := server.ChangeConfiguration(context.Background(), &peerv1.ChangeConfigurationRequest{NewConfig: config}); err != nil {
									t.Error(err)
								} else if !res.Success {
									t.Errorf("[%d] configuration change failed", id)
								}
							}
						}()
					case <-pctx.Done():
						finalMu.Lock()
						final[id] = l.Slice(context.TODO(), 0)
						finalMu.Unlock()
						cancel()
						cls()
						return
					case <-shutdownchs[id]:
						finalMu.Lock()
						final[id] = l.Slice(context.TODO(), 0)
						finalMu.Unlock()
						cancel()
						cls()
						return
					}
				}
			}(int32(i), files[i], dirs[i])
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(5 * time.Second)
			ticker := time.NewTicker(100 * time.Millisecond)

			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					go func() {
						if rand.Intn(5) == 0 {
							// submit
							fmt.Printf("command sending!\n")
							cmd := uuid.NewString()
							for _, commandch := range commandchs {
								commandch <- cmd
							}
							fmt.Printf("command sent!\n")
						}
					}()
				case <-pctx.Done():
					return
				}
			}
		}()

		time.Sleep(10 * time.Second)

		// add server
		wg.Add(1)
		id := int32(peer)
		commandchs[id] = make(chan string)
		go func(file, dir string) {
			defer wg.Done()

			ctx, cancel := context.WithCancel(context.Background())
			server, l, cls := newServer(id, 0, file, dir, nil)

			commitCh := make(chan raft.Entry)
			go func() {
				err := server.Run(ctx, commitCh)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			}()
			fmt.Printf("peer %d started!\n", id)

			for {
				select {
				case <-commitCh:
				case command := <-commandchs[id]:
					go func() {
						if server.Role() == raft.Leader {
							if res, err := server.Submit(context.Background(), &peerv1.SubmitRequest{Command: command}); err != nil {
								fmt.Printf("[%d] %+v", id, err)
							} else if !res.Success {
								t.Errorf("[%d] submit failed", id)
							}
						}
					}()
				case <-pctx.Done():
					finalMu.Lock()
					final[id] = l.Slice(context.TODO(), 0)
					finalMu.Unlock()
					cancel()
					cls()
					return
				}
			}
		}(files[int(id)], dirs[int(id)])

		time.Sleep(3 * time.Second)
		for i := 0; i < peer; i++ {
			fmt.Printf("sending config change to peer %d\n", i)
			configchs[int32(i)] <- &peerv1.Configuration{
				Peers: []*peerv1.Peer{
					{Id: 0, Address: "localhost:50000"},
					{Id: 1, Address: "localhost:50001"},
					{Id: 2, Address: "localhost:50002"},
					{Id: 3, Address: "localhost:50003"},
				},
			}
			fmt.Printf("sent config change to peer %d\n", i)
		}
		time.Sleep(10 * time.Second)

		// add next server
		wg.Add(1)
		id = int32(peer + 1)
		commandchs[id] = make(chan string)
		go func(file, dir string) {
			defer wg.Done()

			ctx, cancel := context.WithCancel(context.Background())
			server, l, cls := newServer(id, 0, file, dir, nil)

			commitCh := make(chan raft.Entry)
			go func() {
				err := server.Run(ctx, commitCh)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			}()
			fmt.Printf("peer %d started!\n", id)

			for {
				select {
				case <-commitCh:
				case command := <-commandchs[id]:
					go func() {
						if server.Role() == raft.Leader {
							if res, err := server.Submit(context.Background(), &peerv1.SubmitRequest{Command: command}); err != nil {
								fmt.Printf("[%d] %+v", id, err)
							} else if !res.Success {
								t.Errorf("[%d] submit failed", id)
							}
						}
					}()
				case <-pctx.Done():
					finalMu.Lock()
					final[id] = l.Slice(context.TODO(), 0)
					finalMu.Unlock()
					cancel()
					cls()
					return
				}
			}
		}(files[int(id)], dirs[int(id)])

		time.Sleep(3 * time.Second)
		for i := 0; i < peer; i++ {
			configchs[int32(i)] <- &peerv1.Configuration{
				Peers: []*peerv1.Peer{
					{Id: 0, Address: "localhost:50000"},
					{Id: 1, Address: "localhost:50001"},
					{Id: 2, Address: "localhost:50002"},
					{Id: 3, Address: "localhost:50003"},
					{Id: 4, Address: "localhost:50004"},
				},
			}
		}
		time.Sleep(10 * time.Second)

		for i := 0; i < peer; i++ {
			configchs[int32(i)] <- &peerv1.Configuration{
				Peers: []*peerv1.Peer{
					{Id: 1, Address: "localhost:50001"},
					{Id: 2, Address: "localhost:50002"},
					{Id: 3, Address: "localhost:50003"},
					{Id: 4, Address: "localhost:50004"},
				},
			}
		}
		shutdownchs[0] <- true
		delete(commandchs, 0)
		finalMu.Lock()
		delete(final, 0)
		finalMu.Unlock()

		time.Sleep(10 * time.Second)

		pcancel()

		wg.Wait()

		// 初めのキーとその値を取得
		var firstKey int32
		var firstValue []raft.Entry
		for key, value := range final {
			firstKey = key
			firstValue = value
			break
		}

		// 全てのエントリを最初のエントリと比較
		for key, value := range final {
			if key == firstKey {
				continue // 最初のエントリは比較しない
			}

			if diff := cmp.Diff(firstValue, value); diff != "" {
				t.Error(diff)
			}
		}

		return
	})
}
