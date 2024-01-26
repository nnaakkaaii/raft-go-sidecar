package raft

import (
	"context"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	workerv1 "github.com/nnaakkaaii/raft-actor-model/proto/worker/v1"
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
	workerv1.WorkerServiceClient
	ID      int32
	Address string
}

func (p *Peer) Reconnect() error {
	conn, err := grpc.Dial(p.Address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	p.WorkerServiceClient = workerv1.NewWorkerServiceClient(conn)
	return nil
}

func NewPeer(id int32, address string) *Peer {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := workerv1.NewWorkerServiceClient(conn)
	return &Peer{
		WorkerServiceClient: client,
		ID:                  id,
		Address:             address,
	}
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
	peers   map[int32]*Peer
	storage Storage
	log     Log

	stateCh           chan State
	getStateCh        chan chan State
	receiveElectionCh chan chan bool
	heartbeatCh       chan chan bool
	commitCh          chan<- Entry

	workerv1.UnimplementedWorkerServiceServer
}

func NewServer(id int32, address string, peers map[int32]*Peer, storage Storage, log Log) *Server {
	return &Server{
		id:      id,
		address: address,
		peers:   peers,
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
	workerv1.RegisterWorkerServiceServer(svr, s)

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

	for {
		select {
		case state := <-stateChangedCh:
			log.Printf("[%d] state changed (%+v)", s.id, state)
			switch {
			case state.role == Leader && role != Leader:
				// promote to leader
				state.leaderAddress = s.address
				for _, peer := range s.peers {
					state.nextIndex[peer.ID] = state.CurrentTerm + 1
					state.matchIndex[peer.ID] = -1
				}
				s.setState(state)
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
			log.Printf("[%d] execute heartbeat", s.id)
			go s.sendAppendEntries(ctx, respCh)
			heartbeat = time.Now()
		case respCh := <-startElectionCh:
			log.Printf("[%d] execute election", s.id)
			go s.sendRequestVotes(ctx, respCh)
		case <-s.receiveElectionCh:
			election = time.Now()
		case <-ctx.Done():
			log.Printf("[%d] final state: (%+v)", s.id, s.getState())
			log.Printf("[%d] cancel context", s.id)
			svr.GracefulStop()
			lis.Close()
			return ctx.Err()
		}
	}
}

func (s *Server) sendAppendEntries(ctx context.Context, respCh chan<- bool) {
	wg := sync.WaitGroup{}
	matchIndexUpdated := make(chan bool, len(s.peers))

	for _, peer := range s.peers {
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
			var entries []*workerv1.LogEntry
			for i, l := range s.log.Slice(ctx, ni) {
				entries = append(entries, &workerv1.LogEntry{
					Term:    l.Term,
					Index:   prevLogIndex + int32(i),
					Command: l.Command,
				})
			}

			res, err := p.AppendEntries(ctx, &workerv1.AppendEntriesRequest{
				Term:         state.CurrentTerm,
				LeaderId:     s.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
			})
			if err != nil {
				log.Printf("[%d] [%d] failed in append entries request: %+v", s.id, p.ID, err)
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
				matchIndex := state.nextIndex[p.ID] - 1
				if state.nextIndex[p.ID] != nextIndex || state.matchIndex[p.ID] != matchIndex {
					state.nextIndex[p.ID] = nextIndex
					state.matchIndex[p.ID] = matchIndex
					s.setState(state)
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
					state.nextIndex[p.ID] = lastIndexOfTerm + 1
				} else {
					state.nextIndex[p.ID] = res.ConflictIndex
				}
				s.setState(state)
				return
			}
			state.nextIndex[p.ID] = res.ConflictIndex
		}(peer)
	}

	go func() {
		count := 1 // リーダー自身を含む
		for range matchIndexUpdated {
			count++
			if count*2 > len(s.peers)+1 {
				respCh <- true
				return
			}
		}
		respCh <- false
	}()

	wg.Wait()
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
			if 2*count >= len(s.peers)+1 {
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
	for i, peer := range s.peers {
		wg.Add(1)
		go func(id int32, p *Peer) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			state := s.getState()

			req := &workerv1.RequestVoteRequest{
				Term:         term,
				CandidateId:  s.id,
				LastLogIndex: state.LastLogIndex,
				LastLogTerm:  state.LastLogTerm,
			}
			log.Printf("[%d] [%d] send request vote - %d", s.id, id, term)
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

func (s *Server) AppendEntries(ctx context.Context, req *workerv1.AppendEntriesRequest) (*workerv1.AppendEntriesResponse, error) {
	state := s.getState()
	if state.leaderAddress != s.peers[req.LeaderId].Address {
		state.leaderAddress = s.peers[req.LeaderId].Address
		s.setState(state)
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
			}
			if len(entries) > 0 {
				state.LastLogIndex = s.log.Extend(ctx, entries, logInsertIndex)
				state.LastLogTerm = entries[len(entries)-1].Term
				s.setState(state)
			}

			return &workerv1.AppendEntriesResponse{
				Term:    state.CurrentTerm,
				Success: true,
			}, nil
		case
			req.PrevLogIndex > state.LastLogIndex:
			return &workerv1.AppendEntriesResponse{
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
			return &workerv1.AppendEntriesResponse{
				Term:          state.CurrentTerm,
				Success:       false,
				ConflictIndex: i + 1,
				ConflictTerm:  conflictTerm,
			}, nil
		}
	} else {
		return &workerv1.AppendEntriesResponse{
			Term:    state.CurrentTerm,
			Success: false,
		}, nil
	}
}

func (s *Server) RequestVote(ctx context.Context, req *workerv1.RequestVoteRequest) (*workerv1.RequestVoteResponse, error) {
	state := s.getState()

	log.Printf("[%d] received request vote - %d, %d", s.id, req.Term, state.CurrentTerm)
	if req.Term > state.CurrentTerm {
		log.Printf("[%d] request vote - become follower", s.id)
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

		log.Printf("[%d] RequestVote 2", s.id)
		log.Printf("[%d] \t - req.Term : %d", s.id, req.Term)
		log.Printf("[%d] \t - state.CurrentTerm : %d", s.id, state.CurrentTerm)
		log.Printf("[%d] \t - req.CandidateId : %d", s.id, req.CandidateId)
		log.Printf("[%d] \t - state.VotedFor : %d", s.id, state.VotedFor)
		log.Printf("[%d] \t - req.LastLogTerm : %d", s.id, req.LastLogTerm)
		log.Printf("[%d] \t - state.LastLogTerm : %d", s.id, state.LastLogTerm)
		log.Printf("[%d] \t - req.LastLogIndex : %d", s.id, req.LastLogIndex)
		log.Printf("[%d] \t - state.LastLogIndex : %d", s.id, state.LastLogIndex)
		return &workerv1.RequestVoteResponse{
			Term:        state.CurrentTerm,
			VoteGranted: false,
		}, nil

	default:
		state.VotedFor = req.CandidateId
		s.setState(state)

		return &workerv1.RequestVoteResponse{
			Term:        state.CurrentTerm,
			VoteGranted: true,
		}, nil
	}
}

func (s *Server) Submit(ctx context.Context, req *workerv1.SubmitRequest) (*workerv1.SubmitResponse, error) {
	state := s.getState()

	if state.role != Leader {
		return &workerv1.SubmitResponse{
			Success: false,
			Address: state.leaderAddress,
		}, nil
	}

	entry := Entry{Term: state.CurrentTerm, Command: req.Command, Index: state.LastLogIndex}
	state.LastLogIndex = s.log.Append(ctx, entry)
	state.LastLogTerm = state.CurrentTerm
	s.commitCh <- entry
	s.setState(state)

	respCh := make(chan bool)
	s.heartbeatCh <- respCh
	resp := <-respCh

	return &workerv1.SubmitResponse{
		Success: resp,
	}, nil
}

func (s *Server) Role() Role {
	return s.getState().role
}
