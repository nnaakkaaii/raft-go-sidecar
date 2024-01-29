package raft

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/nnaakkaaii/raft-gochannel/proto/peer/v1"
)

type Role string

const (
	Leader    Role = "leader"
	Candidate Role = "candidate"
	Follower  Role = "follower"
)

type State struct {
	role          Role
	CurrentTerm   int32
	VotedFor      int32
	LastLogTerm   int32
	LastLogIndex  int32
	leaderAddress string
	nextIndex     map[int32]int32
	matchIndex    map[int32]int32
}

type Peer struct {
	peerv1.PeerServiceClient
	ID      int32
	Address string
}

func (p *Peer) Reconnect() error {
	conn, err := grpc.Dial(p.Address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	p.PeerServiceClient = peerv1.NewPeerServiceClient(conn)
	return nil
}

func NewPeer(id int32, address string) *Peer {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := peerv1.NewPeerServiceClient(conn)
	return &Peer{
		PeerServiceClient: client,
		ID:                id,
		Address:           address,
	}
}

type Peers struct {
	ps map[int32]*Peer
	mu sync.RWMutex
}

func (ps *Peers) SetPeers(peers map[int32]*Peer) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.ps = peers
}

func (ps *Peers) GetPeers() map[int32]*Peer {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return ps.ps
}

type Storage interface {
	LoadState(ctx context.Context, state *State)
	SaveState(ctx context.Context, state *State)
}

type Entry struct {
	Command string
	Index   int32
	Term    int32
}

type Log interface {
	Append(ctx context.Context, entry Entry) int32
	Extend(ctx context.Context, entries []Entry, from int32) int32
	Find(ctx context.Context, index int32) Entry
	Slice(ctx context.Context, from int32) []Entry
}

type Server struct {
	id      int32
	address string
	peers   Peers
	storage Storage
	log     Log

	stateCh           chan State
	getStateCh        chan chan State
	receiveElectionCh chan chan bool
	heartbeatCh       chan chan bool
	commitCh          chan<- Entry
	shutdownCh        chan bool
	changeConfigCh    chan *peerv1.ConfigurationChange

	peerv1.UnimplementedPeerServiceServer
}

func NewServer(id int32, address string, peers map[int32]*Peer, storage Storage, log Log) *Server {
	return &Server{
		id:      id,
		address: address,
		peers:   Peers{ps: peers},
		storage: storage,
		log:     log,
	}
}

func (s *Server) getState() State {
	respCh := make(chan State)
	s.getStateCh <- respCh
	return <-respCh
}

func (s *Server) setState(state State) {
	s.stateCh <- state
}

func (s *Server) electionInterval() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (s *Server) becomeFollower(state *State, term int32) {
	state.role = Follower
	state.CurrentTerm = term
	state.VotedFor = -1
	s.receiveElectionCh <- make(chan bool)
	s.setState(*state)
}

func (s *Server) Run(ctx context.Context, commitCh chan<- Entry) error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	svr := grpc.NewServer()
	peerv1.RegisterPeerServiceServer(svr, s)

	go svr.Serve(lis)
	log.Printf("server listening at %v", lis.Addr())

	// initialize state
	role := Follower
	currentTerm := int32(0)
	initialState := State{
		role:          role,
		CurrentTerm:   currentTerm,
		VotedFor:      -1,
		LastLogTerm:   -1,
		LastLogIndex:  -1,
		leaderAddress: "",
		nextIndex:     map[int32]int32{},
		matchIndex:    map[int32]int32{},
	}
	s.storage.LoadState(ctx, &initialState)

	// sync state
	s.stateCh = make(chan State)
	s.getStateCh = make(chan chan State)
	stateChangedCh := make(chan State)
	go synch(initialState, s.stateCh, s.getStateCh, stateChangedCh)

	// start commit monitoring
	s.commitCh = commitCh

	// timer
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	election := time.Now()
	electionInterval := s.electionInterval()
	startElectionCh := make(chan chan bool)
	s.receiveElectionCh = make(chan chan bool)
	heartbeat := time.Now()
	heartbeatInterval := 50 * time.Millisecond
	s.heartbeatCh = make(chan chan bool)
	s.shutdownCh = make(chan bool)

	for {
		select {
		case state := <-stateChangedCh:
			log.Printf("[%d] state changed (%+v)", s.id, state)
			switch {
			case state.role == Leader && role != Leader:
				// promote to leader
				func() {
					st := s.getState()
					st.leaderAddress = s.address
					for _, peer := range s.peers.GetPeers() {
						st.nextIndex[peer.ID] = st.CurrentTerm + 1
						st.matchIndex[peer.ID] = -1
					}
					s.setState(st)
				}()
			case state.role != Leader:
				election = time.Now()
			}
			role = state.role
			currentTerm = state.CurrentTerm
			go s.storage.SaveState(ctx, &state)
		case <-ticker.C:
			switch {
			case role != Leader &&
				time.Since(election) >= electionInterval:

				electionInterval = s.electionInterval()
				go func() {
					startElectionCh <- make(chan bool)
				}()
			case role == Leader &&
				time.Since(heartbeat) >= heartbeatInterval:
				go func() {
					s.heartbeatCh <- make(chan bool)
				}()
			}
		case respCh := <-s.heartbeatCh:
			// log.Printf("[%d] execute heartbeat", s.id)
			go s.sendAppendEntries(ctx, respCh)
			heartbeat = time.Now()
		case respCh := <-startElectionCh:
			log.Printf("[%d] execute election", s.id)
			go s.sendRequestVotes(ctx, respCh)
		case <-s.receiveElectionCh:
			election = time.Now()
		case <-s.shutdownCh:
			svr.GracefulStop()
			lis.Close()
			log.Printf("[%d] shutting down", s.id)
			return ctx.Err()
		case <-ctx.Done():
			svr.GracefulStop()
			lis.Close()
			log.Printf("[%d] shutting down", s.id)
			return ctx.Err()
		}
	}
}

type indexUpdate struct {
	peerID     int32
	nextIndex  int32
	matchIndex int32
}

func (s *Server) sendAppendEntries(ctx context.Context, respCh chan<- bool) {
	wg := sync.WaitGroup{}
	matchIndexUpdated := make(chan bool, len(s.peers.GetPeers()))
	indexUpdates := make(chan indexUpdate, len(s.peers.GetPeers()))
	peers := s.peers.GetPeers()

	var configChange *peerv1.ConfigurationChange
	select {
	case c := <-s.changeConfigCh:
		configChange = c
	default:
	}

	for _, peer := range peers {
		wg.Add(1)
		go func(p *Peer) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			state := s.getState()
			ni := state.nextIndex[p.ID]
			prevLogIndex := ni - 1
			prevLogTerm := int32(-1)
			if prevLogIndex >= 0 {
				prevLogTerm = s.log.Find(ctx, prevLogIndex).Term
			}
			var entries []*peerv1.LogEntry
			for i, l := range s.log.Slice(ctx, ni) {
				entries = append(entries, &peerv1.LogEntry{
					Term:    l.Term,
					Index:   prevLogIndex + int32(i) + 1,
					Command: l.Command,
				})
			}

			res, err := p.AppendEntries(ctx, &peerv1.AppendEntriesRequest{
				Term:         state.CurrentTerm,
				LeaderId:     s.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				ConfigChange: configChange,
			})
			if err != nil {
				if err := p.Reconnect(); err != nil {
					log.Printf("Error reconnecting to peer: %v", err)
				}
				return
			}

			if res.Term > state.CurrentTerm {
				s.becomeFollower(&state, res.Term)
				return
			}
			if res.Success {
				nextIndex := ni + int32(len(entries))
				matchIndex := nextIndex - 1
				if state.nextIndex[p.ID] != nextIndex || state.matchIndex[p.ID] != matchIndex {
					indexUpdates <- indexUpdate{
						peerID:     p.ID,
						nextIndex:  nextIndex,
						matchIndex: matchIndex,
					}
				}
				if matchIndex == state.LastLogIndex {
					matchIndexUpdated <- true
				}
				return
			}
			if res.ConflictTerm >= 0 {
				lastIndexOfTerm := int32(-1)
				for i := state.LastLogIndex; i >= 0; i-- {
					if s.log.Find(ctx, i).Term == res.ConflictTerm {
						lastIndexOfTerm = i
						break
					}
				}
				if lastIndexOfTerm >= 0 {
					indexUpdates <- indexUpdate{peerID: p.ID, nextIndex: lastIndexOfTerm + 1}
				} else {
					indexUpdates <- indexUpdate{peerID: p.ID, nextIndex: res.ConflictIndex}
				}
				return
			}
			indexUpdates <- indexUpdate{peerID: p.ID, nextIndex: res.ConflictIndex}
		}(peer)
	}

	go func() {
		count := 1 // リーダー自身を含む
		for range matchIndexUpdated {
			count++
			if count*2 > len(s.peers.GetPeers())+1 {
				respCh <- true
				return
			}
		}
		respCh <- false
	}()

	wg.Wait()
	close(matchIndexUpdated)

	state := s.getState()
	updateFlag := false
	for update := range indexUpdates {
		updateFlag = true
		state.nextIndex[update.peerID] = update.nextIndex
		state.matchIndex[update.peerID] = update.matchIndex
	}
	if updateFlag {
		s.setState(state)
	}
	close(matchIndexUpdated)
}

func (s *Server) sendRequestVotes(ctx context.Context, respCh chan<- bool) {
	st := s.getState()
	st.role = Candidate
	st.CurrentTerm += 1
	st.VotedFor = s.id
	s.setState(st)
	s.receiveElectionCh <- make(chan bool)
	term := st.CurrentTerm

	votes := make(chan bool)
	go func(ch chan<- bool) {
		count := 1
		for vote := range votes {
			if vote {
				count++
			}
			if 2*count >= len(s.peers.GetPeers())+1 {
				state := s.getState()
				state.role = Leader
				s.setState(state)
				ch <- true
				return
			}
		}
		ch <- false
	}(respCh)

	demotes := make(chan int32)

	wg := sync.WaitGroup{}
	for i, peer := range s.peers.GetPeers() {
		wg.Add(1)
		go func(id int32, p *Peer) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			state := s.getState()

			req := &peerv1.RequestVoteRequest{
				Term:         term,
				CandidateId:  s.id,
				LastLogIndex: state.LastLogIndex,
				LastLogTerm:  state.LastLogTerm,
			}
			res, err := p.RequestVote(ctx, req)
			if err != nil {
				log.Printf("[%d] [%d] %+v", s.id, id, err)
				if err := p.Reconnect(); err != nil {
					log.Printf("Error reconnecting to peer: %v", err)
				}
				return
			}
			if res.Term > state.CurrentTerm {
				demotes <- res.Term
			}
			votes <- res.VoteGranted
		}(i, peer)
	}

	go func() {
		for demote := range demotes {
			state := s.getState()
			s.becomeFollower(&state, demote)
			return
		}
	}()

	wg.Wait()
	close(votes)
	close(demotes)
}

func (s *Server) AppendEntries(ctx context.Context, req *peerv1.AppendEntriesRequest) (*peerv1.AppendEntriesResponse, error) {
	state := s.getState()
	peers := s.peers.GetPeers()
	if state.leaderAddress != peers[req.LeaderId].Address {
		state.leaderAddress = peers[req.LeaderId].Address
		s.setState(state)
	}

	if req.ConfigChange != nil {
		// joint consensus
		if req.ConfigChange.OldConfig != nil && req.ConfigChange.NewConfig != nil {
			union := append(req.ConfigChange.NewConfig.Peers, req.ConfigChange.OldConfig.Peers...)
			for _, p := range union {
				if p.Id == s.id {
					if p.Address != s.address {
						return &peerv1.AppendEntriesResponse{
							Term:    state.CurrentTerm,
							Success: false,
						}, errors.New("existing peer address cannot be changed")
					}
					continue
				}
				if existingPeer, ok := peers[p.Id]; ok {
					if existingPeer.Address != p.Address {
						return &peerv1.AppendEntriesResponse{
							Term:    state.CurrentTerm,
							Success: false,
						}, errors.New("existing peer address cannot be changed")
					}
					continue
				}
				peers[p.Id] = NewPeer(p.Id, p.Address)
			}
		}
		if req.ConfigChange.OldConfig == nil && req.ConfigChange.NewConfig != nil {
			if !containsPeer(req.ConfigChange.NewConfig.Peers, s.id) {
				go func() {
					s.shutdownCh <- true
				}()
				return &peerv1.AppendEntriesResponse{
					Term:    state.CurrentTerm,
					Success: true,
				}, nil
			}
			for id := range peers {
				if !containsPeer(req.ConfigChange.NewConfig.Peers, id) {
					delete(peers, id)
				}
			}
		}
		s.peers.SetPeers(peers)
	}

	if req.Term > state.CurrentTerm {
		s.becomeFollower(&state, req.Term)
	}
	if req.Term == state.CurrentTerm {
		if state.role != Follower {
			s.becomeFollower(&state, req.Term)
		}
		s.receiveElectionCh <- make(chan bool)

		switch {
		case
			req.PrevLogIndex == -1,
			req.PrevLogIndex <= state.LastLogIndex &&
				req.PrevLogTerm == s.log.Find(ctx, req.PrevLogIndex).Term:
			logInsertIndex := req.PrevLogIndex + 1

			var newEntriesIndex int
			for newEntriesIndex = 0; newEntriesIndex < len(req.Entries); newEntriesIndex++ {
				if logInsertIndex <= state.LastLogIndex &&
					s.log.Find(ctx, logInsertIndex).Term == req.Entries[newEntriesIndex].Term {
					logInsertIndex++
				} else {
					break
				}
			}

			entries := make([]Entry, 0, len(req.Entries[newEntriesIndex:]))
			for i, e := range req.Entries[newEntriesIndex:] {
				entry := Entry{Term: e.Term, Command: e.Command, Index: logInsertIndex + int32(i)}
				entries = append(entries, entry)
				s.commitCh <- entry
				log.Printf("[%d] has commit entry %+v from %d", s.id, entry, req.LeaderId)
			}
			if len(entries) > 0 {
				state.LastLogIndex = s.log.Extend(ctx, entries, logInsertIndex)
				state.LastLogTerm = entries[len(entries)-1].Term
				s.setState(state)
			}

			return &peerv1.AppendEntriesResponse{
				Term:    state.CurrentTerm,
				Success: true,
			}, nil
		case
			req.PrevLogIndex > state.LastLogIndex:
			return &peerv1.AppendEntriesResponse{
				Term:          state.CurrentTerm,
				Success:       false,
				ConflictIndex: state.LastLogIndex + 1,
				ConflictTerm:  -1,
			}, nil
		default:
			conflictTerm := s.log.Find(ctx, req.PrevLogIndex).Term
			var i int32
			for i = req.PrevLogIndex - 1; i >= 0; i-- {
				if s.log.Find(ctx, i).Term != conflictTerm {
					break
				}
			}
			return &peerv1.AppendEntriesResponse{
				Term:          state.CurrentTerm,
				Success:       false,
				ConflictIndex: i + 1,
				ConflictTerm:  conflictTerm,
			}, nil
		}
	} else {
		return &peerv1.AppendEntriesResponse{
			Term:    state.CurrentTerm,
			Success: false,
		}, nil
	}
}

func (s *Server) RequestVote(ctx context.Context, req *peerv1.RequestVoteRequest) (*peerv1.RequestVoteResponse, error) {
	state := s.getState()

	if req.Term > state.CurrentTerm {
		s.becomeFollower(&state, req.Term)
	}

	switch {
	case
		// If the term in the request is less than the server's current term,
		// the server rejects the vote because the candidate is outdated
		// compared to the server's view of the term.
		req.Term < state.CurrentTerm,
		// If the server has already voted for another candidate in the current
		// term, it rejects the vote to uphold the Raft rule that a server can
		// vote for only one candidate per term.
		state.VotedFor != -1 && state.VotedFor != req.CandidateId,
		// The server rejects the vote if the candidate's log is less
		// up-to-date than its own log. Raft ensures that the leader has the
		// most up-to-date log; hence, a server votes only for candidates whose
		// logs are at least as up-to-date as its own.
		req.LastLogTerm < state.LastLogTerm,
		// Similarly, if the logs are in the same term but the candidate's log
		// is shorter (has a smaller index), the server rejects the vote.
		// This also ensures that the leader's log is the most up-to-date.
		req.LastLogTerm <= state.LastLogTerm &&
			req.LastLogIndex < state.LastLogIndex:

		return &peerv1.RequestVoteResponse{
			Term:        state.CurrentTerm,
			VoteGranted: false,
		}, nil

	default:
		state.VotedFor = req.CandidateId
		s.setState(state)

		return &peerv1.RequestVoteResponse{
			Term:        state.CurrentTerm,
			VoteGranted: true,
		}, nil
	}
}

func (s *Server) Submit(ctx context.Context, req *peerv1.SubmitRequest) (*peerv1.SubmitResponse, error) {
	log.Printf("[%d] received command", s.id)
	state := s.getState()

	if state.role != Leader {
		return &peerv1.SubmitResponse{
			Success: false,
			Address: state.leaderAddress,
		}, nil
	}

	entry := Entry{Term: state.CurrentTerm, Command: req.Command, Index: state.LastLogIndex + 1}
	state.LastLogIndex = s.log.Append(ctx, entry)
	state.LastLogTerm = state.CurrentTerm
	s.commitCh <- entry
	log.Printf("[%d] has commit entry %+v as a leader", s.id, entry)
	s.setState(state)

	respCh := make(chan bool)
	go func() {
		s.heartbeatCh <- respCh
	}()
	select {
	case resp := <-respCh:
		log.Printf("[%d] has successfully broadcasted entry %+v as a leader", s.id, entry)
		return &peerv1.SubmitResponse{
			Success: resp,
		}, nil
	case <-ctx.Done():
		return &peerv1.SubmitResponse{
			Success: false,
		}, ctx.Err()
	}
}

func (s *Server) ChangeConfiguration(ctx context.Context, req *peerv1.ChangeConfigurationRequest) (*peerv1.ChangeConfigurationResponse, error) {
	log.Printf("[%d] change configuration", s.id)
	state := s.getState()

	if state.role != Leader {
		return &peerv1.ChangeConfigurationResponse{
			Success: false,
			Address: state.leaderAddress,
		}, nil
	}
	if req.NewConfig == nil || req.NewConfig.Peers == nil || len(req.NewConfig.Peers) == 0 {
		return &peerv1.ChangeConfigurationResponse{
			Success: false,
		}, nil
	}

	phase1RespCh := make(chan bool)
	phase2RespCh := make(chan bool)
	go func() {
		peers := s.peers.GetPeers()
		oldConfig := []*peerv1.Peer{{Id: s.id, Address: s.address}}
		for _, p := range peers {
			oldConfig = append(oldConfig, &peerv1.Peer{Id: p.ID, Address: p.Address})
		}
		s.changeConfigCh <- &peerv1.ConfigurationChange{
			OldConfig: &peerv1.Configuration{Peers: oldConfig},
			NewConfig: req.NewConfig,
		}
		s.heartbeatCh <- phase1RespCh
		s.changeConfigCh <- &peerv1.ConfigurationChange{
			NewConfig: req.NewConfig,
		}
		s.heartbeatCh <- phase2RespCh
	}()
	select {
	case resp := <-phase1RespCh:
		if resp {
			log.Printf("[%d] has successfully broadcasted 1st change configuration as a leader", s.id)
		} else {
			log.Printf("[%d] has failed to broadcast 1st change configuration as a leader", s.id)
			return &peerv1.ChangeConfigurationResponse{
				Success: false,
			}, nil
		}
	case <-ctx.Done():
		return &peerv1.ChangeConfigurationResponse{
			Success: false,
		}, ctx.Err()
	}
	select {
	case resp := <-phase2RespCh:
		if resp {
			log.Printf("[%d] has successfully broadcasted 2nd change configuration as a leader", s.id)
		} else {
			log.Printf("[%d] has failed to broadcasted 2nd change configuration as a leader", s.id)
		}
		return &peerv1.ChangeConfigurationResponse{
			Success: resp,
		}, nil
	case <-ctx.Done():
		return &peerv1.ChangeConfigurationResponse{
			Success: false,
		}, ctx.Err()
	}
}

func (s *Server) Role() Role {
	return s.getState().role
}
