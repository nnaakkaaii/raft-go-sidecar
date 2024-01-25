package main

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

func main() {
	lis, err := net.Listen("tcp", ":50000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	workerv1.RegisterWorkerServiceServer(s, &Server{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type Role string

const (
	Leader    Role = "leader"
	Candidate Role = "candidate"
	Follower  Role = "follower"
)

type State struct {
	role          Role
	currentTerm   int32
	votedFor      int32
	lastLogTerm   int32
	lastLogIndex  int32
	leaderAddress string
	nextIndex     map[int32]int32
	matchIndex    map[int32]int32
}

type Peer struct {
	workerv1.WorkerServiceClient
	ID      int32
	Address string
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

	stateCh     chan State
	getStateCh  chan chan State
	heartbeatCh chan chan bool
	commitCh    chan<- Entry

	workerv1.UnimplementedWorkerServiceServer
}

func NewServer(id int32, peers map[int32]*Peer, storage Storage, log Log) *Server {
	svr := &Server{
		id:      id,
		peers:   peers,
		storage: storage,
		log:     log,
	}
	return svr
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

func (s *Server) Run(ctx context.Context, commitCh chan<- Entry) error {
	// initialize state
	role := Follower
	currentTerm := int32(0)
	initialState := State{
		role:          role,
		currentTerm:   currentTerm,
		votedFor:      -1,
		lastLogTerm:   -1,
		lastLogIndex:  -1,
		leaderAddress: "",
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

	lastTerm := currentTerm
	election := time.Now()
	electionInterval := s.electionInterval()
	electionCh := make(chan chan bool)
	heartbeat := time.Now()
	heartbeatInterval := 50 * time.Millisecond
	s.heartbeatCh = make(chan chan bool)

	for {
		select {
		case state := <-stateChangedCh:
			switch {
			case state.role == Leader && role != Leader:
				// promote to leader
				state.leaderAddress = s.address
				for _, peer := range s.peers {
					state.nextIndex[peer.ID] = state.currentTerm + 1
					state.matchIndex[peer.ID] = -1
				}
				s.setState(state)
			case state.role != Leader:
				election = time.Now()
			}
			role = state.role
			currentTerm = state.currentTerm
			go s.storage.SaveState(ctx, &state)
		case <-ticker.C:
			switch {
			case role != Leader &&
				currentTerm-lastTerm == 0 &&
				time.Since(election) >= electionInterval:
				lastTerm = currentTerm
				electionInterval = s.electionInterval()

				respCh := make(chan bool)
				electionCh <- respCh
			case role == Leader &&
				time.Since(heartbeat) >= heartbeatInterval:

				respCh := make(chan bool)
				s.heartbeatCh <- respCh
			}
		case respCh := <-s.heartbeatCh:
			go s.sendAppendEntries(ctx, respCh)
			heartbeat = time.Now()
		case respCh := <-electionCh:
			go s.sendRequestVotes(ctx, lastTerm, respCh)
		case <-ctx.Done():
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
				Term:         state.currentTerm,
				LeaderId:     s.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
			})
			if err != nil {
				log.Printf("[%d] %+v", p.ID, err)
				return
			}

			if res.Term > state.currentTerm {
				state.role = Follower
				state.currentTerm = res.Term
				state.votedFor = -1
				s.setState(state)
				return
			}
			if res.Success {
				state.nextIndex[p.ID] = ni + int32(len(entries))
				state.matchIndex[p.ID] = state.nextIndex[p.ID] - 1
				if state.matchIndex[p.ID] == state.lastLogIndex {
					matchIndexUpdated <- true
				}
				s.setState(state)
				return
			}
			if res.ConflictTerm >= 0 {
				lastIndexOfTerm := int32(-1)
				for i := state.lastLogIndex; i >= 0; i-- {
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

func (s *Server) sendRequestVotes(ctx context.Context, term int32, respCh chan<- bool) {
	votes := make(chan bool)
	go func(ch chan<- bool) {
		var count int
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

			state := s.getState()

			req := &workerv1.RequestVoteRequest{
				Term:         term,
				CandidateId:  s.id,
				LastLogIndex: state.lastLogIndex,
				LastLogTerm:  state.lastLogTerm,
			}
			res, err := p.RequestVote(ctx, req)
			if err != nil {
				log.Printf("[%d] %+v", id, err)
				return
			}
			if res.Term > state.currentTerm {
				demotes <- res.Term
			}
			votes <- res.VoteGranted
		}(i, peer)
	}

	go func() {
		for demote := range demotes {
			state := s.getState()
			state.role = Follower
			state.votedFor = -1
			state.currentTerm = demote
			s.setState(state)
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

	if req.Term > state.currentTerm {
		state.currentTerm = req.Term
		s.setState(state)
		return &workerv1.AppendEntriesResponse{
			Term:    state.currentTerm,
			Success: false,
		}, nil
	} else if req.Term == state.currentTerm {
		if state.role != Follower {
			state.role = Follower
			state.currentTerm = req.Term
			state.votedFor = -1
			s.setState(state)
		}

		switch {
		case
			req.PrevLogIndex == -1,
			req.PrevLogIndex <= state.lastLogIndex &&
				req.PrevLogTerm == s.log.Find(ctx, req.PrevLogIndex).Term:
			logInsertIndex := req.PrevLogIndex + 1

			var newEntriesIndex int
			for newEntriesIndex = 0; newEntriesIndex < len(req.Entries); newEntriesIndex++ {
				if logInsertIndex <= state.lastLogIndex &&
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
			state.lastLogIndex = s.log.Extend(ctx, entries, logInsertIndex)
			state.lastLogTerm = entries[len(entries)-1].Term
			s.setState(state)

			return &workerv1.AppendEntriesResponse{
				Term:    state.currentTerm,
				Success: true,
			}, nil
		case
			req.PrevLogIndex > state.lastLogIndex:
			return &workerv1.AppendEntriesResponse{
				Term:          state.currentTerm,
				Success:       false,
				ConflictIndex: state.lastLogIndex + 1,
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
				Term:          state.currentTerm,
				Success:       false,
				ConflictIndex: i + 1,
				ConflictTerm:  conflictTerm,
			}, nil
		}
	} else {
		return &workerv1.AppendEntriesResponse{
			Term:    state.currentTerm,
			Success: false,
		}, nil
	}
}

func (s *Server) RequestVote(ctx context.Context, req *workerv1.RequestVoteRequest) (*workerv1.RequestVoteResponse, error) {
	state := s.getState()

	switch {
	case
		// if a server discovers that its term is out of date (i.e., there is a
		// candidate with a higher term), it immediately becomes a follower.
		// This is because the server acknowledges that there is a more
		// up-to-date candidate or leader in the cluster. By becoming a
		// follower, the server resets its state according to the higher term
		// and participates in the new election or follows the new leader.
		req.Term > state.currentTerm:

		state.role = Follower
		state.currentTerm = req.Term
		state.votedFor = -1
		s.setState(state)

		return &workerv1.RequestVoteResponse{
			Term:        state.currentTerm,
			VoteGranted: false,
		}, nil
	case
		// If the term in the request is less than the server's current term,
		// the server rejects the vote because the candidate is outdated
		// compared to the server's view of the term.
		req.Term < state.currentTerm,
		// If the server has already voted for another candidate in the current
		// term, it rejects the vote to uphold the Raft rule that a server can
		// vote for only one candidate per term.
		state.votedFor != -1 && state.votedFor != req.CandidateId,
		// The server rejects the vote if the candidate's log is less
		// up-to-date than its own log. Raft ensures that the leader has the
		// most up-to-date log; hence, a server votes only for candidates whose
		// logs are at least as up-to-date as its own.
		req.LastLogTerm < state.lastLogTerm,
		// Similarly, if the logs are in the same term but the candidate's log
		// is shorter (has a smaller index), the server rejects the vote.
		// This also ensures that the leader's log is the most up-to-date.
		req.LastLogTerm == state.lastLogTerm &&
			req.LastLogIndex >= state.lastLogIndex:

		return &workerv1.RequestVoteResponse{
			Term:        state.currentTerm,
			VoteGranted: false,
		}, nil

	default:
		state.votedFor = req.CandidateId
		s.setState(state)

		return &workerv1.RequestVoteResponse{
			Term:        state.currentTerm,
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

	entry := Entry{Term: state.currentTerm, Command: req.Command, Index: state.lastLogIndex}
	state.lastLogIndex = s.log.Append(ctx, entry)
	state.lastLogTerm = state.currentTerm
	s.commitCh <- entry
	s.setState(state)

	respCh := make(chan bool)
	s.heartbeatCh <- respCh
	resp := <-respCh

	return &workerv1.SubmitResponse{
		Success: resp,
	}, nil
}
