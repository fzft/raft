package raft

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Role int64

const (
	Joining Role = iota
	Observer
	Follower
	Candidate
	Leader
	Failed
	Leaving
)

type RequestVoteArgs struct {
	Term        int64
	CandidateId int64
}
type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int64
	LeaderId int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

type Peer struct {
	id int64
}

type Engine struct {
	mu sync.Mutex

	id          int64
	peers       map[int64]Peer
	role        Role
	votedFor    int64
	currentTerm int64
	srv         *Server
	leaderId    int64

	electionTimeout int64
}

func NewEngine(id int64, s *Server) *Engine {
	return &Engine{
		id:  id,
		srv: s,
	}
}

func (e *Engine) start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.role = Follower
	e.votedFor = -1
	e.electionTimeout = 10
	go e.runPeriodTasks()
}

func (e *Engine) stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.role = Leaving
	log.Infof("raft stopped")
}

func (e *Engine) rescheduleElection() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.electionTimeout = time.Now().UnixMilli() + 2500
}

func (e *Engine) runPeriodTasks() {
	for e.role != Leaving {
		if e.role != Leaving {
			e.role = Failed
		}
		switch e.role {
		case Joining:
			e.role = Follower
			e.rescheduleElection()
			break
		case Observer:
			break
		case Follower:
			break
		case Candidate:
			if time.Now().UnixMilli() > e.electionTimeout {
				e.callElection()
			}
			break

		}
	}
}

func (e *Engine) callElection() {
	if e.role == Leaving {
		return
	}
	votesNeeded := (len(e.peers) + 1) / 2
	votes := 1
	e.role = Candidate
	e.currentTerm++
	e.leaderId = 0
	e.votedFor = e.id
	log.Infof("%d is calling en election (term %d)", e.id, e.currentTerm)
	for _, p := range e.peers {
		go func(p Peer) {
			args := &RequestVoteArgs{
				Term:        e.currentTerm,
				CandidateId: e.id,
			}

			var reply *RequestVoteReply

			if err := e.srv.Call(p.id, "Engine.SendRequestVote", args, reply); err != nil {
				e.mu.Lock()
				defer e.mu.Unlock()
				if !e.stepDown(reply.Term) {
					if reply.Term == e.currentTerm && e.role == Candidate {
						if reply.VoteGranted {
							votes++
						}
						if votes >= votesNeeded {
							e.becomeLeader()
						}
					}
				}
			}
		}(p)
	}
}

func (e *Engine) SendRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.role == Leaving {
		return nil
	}

	if e.currentTerm < args.Term {
		log.Infof("%d term out of date (term %d < %d)", e.id, e.currentTerm, args.Term)
		e.becomeFollower(args.Term)
		return nil
	}

	if e.currentTerm == args.Term && (e.votedFor == -1 || e.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		e.votedFor = args.CandidateId
		e.rescheduleElection()
	} else {
		reply.VoteGranted = false
	}
	log.Infof("%d request vote response %v", e.id, reply)
	reply.Term = e.currentTerm
	return nil
}

func (e *Engine) SendAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.role == Leaving {
		return nil
	}

	if e.currentTerm < args.Term {
		log.Infof("%d term out of date (term %d < %d)", e.id, e.currentTerm, args.Term)
		e.becomeFollower(args.Term)
		return nil
	}
	reply.Success = false
	if args.Term == e.currentTerm {
		if e.role != Follower {
			e.becomeFollower(args.Term)
		}
		e.rescheduleElection()
		reply.Success = true
	}

	reply.Term = e.currentTerm
	log.Infof("%d request append entries response %v", e.id, reply)
	return nil
}

func (e *Engine) stepDown(term int64) bool {
	if term > e.currentTerm {
		e.currentTerm = term
		if e.role == Candidate || e.role == Leader {
			log.Infof("%d is stepping down (term %d)", e.id, e.currentTerm)
			e.role = Follower
		}
		e.rescheduleElection()
		return true
	}
	return false
}

func (e *Engine) becomeLeader() {
	log.Infof("%d is becoming the leader (term %d)", e.id, e.currentTerm)
	e.role = Leader
	e.leaderId = e.id
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			e.updatePeers()
			<-ticker.C
			e.mu.Lock()
			if e.role != Leader {
				e.mu.Unlock()
				return
			}
			e.mu.Unlock()
		}
	}()

}

func (e *Engine) becomeFollower(term int64) {
	log.Infof("%d becoming the follower (term %d)", e.id, term)
	e.currentTerm = term
	e.role = Follower
	e.votedFor = -1
	e.rescheduleElection()

}

func (e *Engine) updatePeers() {
	e.mu.Lock()
	savedCurrentTerm := e.currentTerm
	e.mu.Unlock()
	for _, p := range e.peers {
		log.Infof("sending AppendEntries to %d:", p.id)
		go func() {
			args := &AppendEntriesArgs{
				Term:     e.currentTerm,
				LeaderId: e.id,
			}

			var reply *AppendEntriesReply

			if err := e.srv.Call(p.id, "Engine.SendAppendEntries", args, reply); err != nil {
				if e.stepDown(savedCurrentTerm) {
					log.Infof("%d term out of date (term %d < %d)", e.id, savedCurrentTerm, reply.Term)
					e.becomeFollower(reply.Term)
				}
			}
		}()
	}
}
