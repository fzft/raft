package raft

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
		DisableQuote:    true,
	})
}

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
	Term         int64
	CandidateId  int64
	LastLogIndex int64
	LastLogTerm  int64
}
type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []Entry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term         int64
	Success      bool
	LastLogIndex int64
}

type Peer struct {
	id               int64
	nextIndex        int64
	matchIndex       int64
	lastAppendMillis int64
	fresh            bool
}

// Engine implements Raft Consensus
type Engine struct {
	mu sync.Mutex

	// election field
	id          int64
	peers       map[int64]*Peer
	role        Role
	votedFor    int64
	currentTerm int64
	srv         *Server
	leaderId    int64

	// command field
	log                *Log
	firstIndexOfTerm   int64
	lastTermCommitted  int64
	electionResetEvent time.Time
}

func NewEngine(id int64, s *Server, peerIds []int64, ready chan struct{}) *Engine {
	e := &Engine{
		id:       id,
		srv:      s,
		role:     Follower,
		peers:    make(map[int64]*Peer),
		votedFor: -1,
		log:      NewLog(id, NewConfig()),
	}

	e.addPeers(peerIds...)
	go func() {
		<-ready
		e.mu.Lock()
		e.electionResetEvent = time.Now()
		e.mu.Unlock()
		e.runPeriodTasks()
	}()
	return e
}

func (e *Engine) SendRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.role == Leaving {
		return nil
	}

	if e.currentTerm < args.Term {
		log.Infof("[%d] engine term out of date (term %d < %d)", e.id, e.currentTerm, args.Term)
		e.becomeFollower(args.Term)
	}

	if e.currentTerm == args.Term && (e.votedFor == -1 || e.votedFor == args.CandidateId) &&
		args.LastLogIndex >= e.log.LastIndex() && args.LastLogTerm >= e.log.LastTerm() {
		reply.VoteGranted = true
		e.votedFor = args.CandidateId
		e.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = e.currentTerm
	log.Infof("[%d] engine request vote response %+v", e.id, reply)
	return nil
}

func (e *Engine) SendAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	defer func() {
		reply.LastLogIndex = e.log.LastIndex()
	}()

	if e.role == Leaving {
		return nil
	}

	if e.currentTerm < args.Term {
		log.Infof("[%d] engine term out of date (term %d < %d)", e.id, e.currentTerm, args.Term)
		e.becomeFollower(args.Term)
		return nil
	}
	reply.Success = false
	defer func() {
		reply.Term = e.currentTerm
		log.Tracef("[%d] engine request append entries response %+v", e.id, reply)
	}()
	if args.Term == e.currentTerm {
		if e.role != Follower {
			e.becomeFollower(args.Term)
		}
		log.Tracef("[%d] engine reset election time", e.id)
		e.electionResetEvent = time.Now()

		if e.log.IsConsistentWith(args.PrevLogIndex, args.PrevLogTerm) {
			for _, entry := range args.Entries {
				if ok := e.log.append(entry); !ok {
					log.Warnf("[%d] engine is failing append entries leader: %+v", e.id, entry)
					reply.Success = false
					return nil
				}
			}
			e.log.SetCommitIndex(Min(args.LeaderCommit, e.log.LastIndex()))
			log.Tracef("[%d] engine is fine with append entries from leader", e.id)
			reply.Success = true
			return nil
		}
		log.Warnf("[%d] engine is failing with inconsistent append entries from leader", e.id)
		log.Warnf("[%d] engine Leader prevLogTerm=%d , prevLogIndex=%d", e.id, args.PrevLogTerm, args.PrevLogIndex)
		log.Warnf("[%d] engine Follower firstTerm=%d, firstIndex=%d", e.id, e.log.FirstTerm(), e.log.FirstIndex())
		log.Warnf("[%d] engine Follower lastTerm=%d, lastIndex=%d", e.id, e.log.LastTerm(), e.log.LastIndex())
	}

	log.Tracef("[%d] engine is rejecting append entries from leader", e.id)
	reply.Success = false
	return nil
}

func (e *Engine) addPeers(ids ...int64) {
	for _, id := range ids {
		if _, ok := e.peers[id]; !ok {
			e.peers[id] = &Peer{id: id}
		}
	}
}

func (e *Engine) executeCommand(command TestCommand) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Infof("[%d] engine recv command %v in [%d] role", e.id, command, e.role)
	if e.role == Leader {
		return e.log.Append(e.currentTerm, command)
	}
	return false
}

func (e *Engine) report() (id int64, term int64, isLeader bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.id, e.currentTerm, e.role == Leader
}

func (e *Engine) stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.role = Leaving
	log.Infof("[%d] engine raft stopped", e.id)
}

func (e *Engine) runPeriodTasks() {
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	e.mu.Lock()
	termStarted := e.currentTerm
	e.mu.Unlock()

	log.Infof("[%d] engine election started in (%v), term=%d", e.id, electionTimeout, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		e.mu.Lock()

		if e.role != Candidate && e.role != Follower {
			log.Infof("[%d] engine in election, role=%d, out", e.id, e.role)
			e.mu.Unlock()
			return
		}

		if termStarted != e.currentTerm {
			log.Infof("[%d] engine in election term change from %d to %d, out", e.id, termStarted, e.currentTerm)
			e.mu.Unlock()
			return
		}

		if elapsed := time.Since(e.electionResetEvent); elapsed > electionTimeout {
			e.callElection()
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
	}
}

func (e *Engine) callElection() {
	if e.role == Leaving {
		return
	}
	votesNeeded := (len(e.peers) + 1) / 2
	votes := 0
	e.role = Candidate
	e.currentTerm++
	savedCurrentTerm := e.currentTerm
	e.leaderId = 0
	e.votedFor = e.id
	e.electionResetEvent = time.Now()

	log.Infof("[%d] engine becomes Candidate (currentTerm=%d)", e.id, e.currentTerm)
	for _, p := range e.peers {
		go func(p *Peer) {
			args := &RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  e.id,
				LastLogTerm:  e.log.LastTerm(),
				LastLogIndex: e.log.LastIndex(),
			}

			reply := new(RequestVoteReply)

			if err := e.srv.Call(p.id, "Engine.SendRequestVote", args, reply); err == nil {
				e.mu.Lock()
				defer e.mu.Unlock()
				if !e.stepDown(reply.Term) {
					if reply.Term == savedCurrentTerm && e.role == Candidate {
						if reply.VoteGranted {
							votes++
						}
						if votes >= votesNeeded {
							e.becomeLeader()
						}
					}
				} else {
					log.Infof("[%d] engine term out of date (term %d)", e.id, reply.Term)
					e.becomeFollower(reply.Term)
				}
			} else {
				log.Infof("[%d] engine rpc [Engine.SendRequestVote] got error: %s", e.id, err.Error())
			}
		}(p)
	}
}

func (e *Engine) stepDown(term int64) bool {
	if term > e.currentTerm {
		e.currentTerm = term
		if e.role == Candidate || e.role == Leader {
			log.Infof("[%d] engine is stepping down (term %d)", e.id, e.currentTerm)
			e.role = Follower
		}
		e.electionResetEvent = time.Now()
		return true
	}
	return false
}

func (e *Engine) becomeLeader() {
	log.Infof("[%d] engine is becoming the leader (term %d)", e.id, e.currentTerm)
	e.role = Leader
	e.leaderId = e.id
	e.firstIndexOfTerm = e.log.LastIndex() + 1
	for _, p := range e.peers {
		p.matchIndex = 0
		p.nextIndex = e.log.LastIndex() + 1
	}

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			e.updatePeers()
			<-ticker.C
			e.updateCommitIndex()

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
	log.Infof("[%d] engine becoming the follower (term %d)", e.id, term)
	e.currentTerm = term
	e.role = Follower
	e.votedFor = -1
	e.electionResetEvent = time.Now()
	go e.runPeriodTasks()
}

func (e *Engine) updatePeers() {
	e.mu.Lock()
	savedCurrentTerm := e.currentTerm
	e.mu.Unlock()
	for _, p := range e.peers {
		go func(p *Peer) {
			e.mu.Lock()
			prevLogIndex := p.nextIndex - 1
			prevLogTerm, _ := e.log.getTerm(prevLogIndex)
			var entries []Entry
			if prevLogIndex > -1 {
				entries = e.log.GetEntries(prevLogIndex)
			}
			args := &AppendEntriesArgs{
				Term:         e.currentTerm,
				LeaderId:     e.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: e.log.CommitIndex(),
			}
			e.mu.Unlock()

			reply := new(AppendEntriesReply)

			if err := e.srv.Call(p.id, "Engine.SendAppendEntries", args, reply); err == nil {
				e.mu.Lock()
				defer e.mu.Unlock()
				if e.stepDown(reply.Term) {
					log.Infof("%d term out of date (term %d < %d)", e.id, savedCurrentTerm, reply.Term)
					e.becomeFollower(reply.Term)
				} else {
					p.fresh = false
					if reply.Success {
						entries = args.Entries
						if len(entries) > 0 {
							p.matchIndex = entries[len(entries)-1].Index
							p.nextIndex = p.matchIndex + 1
						} else {
							p.nextIndex = Max(reply.LastLogIndex+1, 1)
						}
						log.Tracef("[%d] engine leader appendEntries reply from %d, success, nextIndex: %d, matchIndex: %d, (lastLogIndex=%d)", e.id, p.id, p.nextIndex, p.matchIndex, reply.LastLogIndex)
					} else {
						if p.nextIndex > reply.LastLogIndex {
							p.nextIndex = Max(reply.LastLogIndex+1, 1)
						} else if p.nextIndex > 1 {
							p.nextIndex--
						}
						log.Tracef("[%d] engine leader appendEntries reply from %d, failure, nextIndex:%d", e.id, p.id, p.nextIndex)
					}
				}
			} else {
				log.Infof("[%d] engine rpc [Engine.SendAppendEntries] got error: %s", e.id, err.Error())
			}
		}(p)
	}
}

func (e *Engine) updateCommitIndex() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.role == Leader {
		if e.isCommittable(e.firstIndexOfTerm) {
			index := e.log.LastIndex()
			for _, p := range e.peers {
				index = Min(index, p.matchIndex)
			}
			index = Max(index, e.log.CommitIndex())
			for index <= e.log.LastIndex() && e.isCommittable(index) {
				if entry, ok := e.log.Entry(index); ok && entry.Term != e.lastTermCommitted {
					log.Infof("[%d] engine committed new term %d", e.id, entry.Term)
					for _, p := range e.peers {
						log.Infof("[%d] engine's peer [%d] has matched %d >= %d (%t)", e.id, p.id, p.matchIndex, e.firstIndexOfTerm, p.matchIndex >= e.firstIndexOfTerm)
						e.lastTermCommitted = entry.Term
					}
				}
				e.log.SetCommitIndex(index)
				log.Tracef("[%d] engine leader set commitIndex %d", e.id, index)
				index++
			}
		}
	}
}

func (e *Engine) isCommittable(index int64) bool {
	var (
		count  = 1
		needed = (1 + len(e.peers)) / 2
	)

	for _, p := range e.peers {
		if p.matchIndex >= index {
			count++
			if count >= needed {
				return true
			}
		}
	}
	return count >= needed
}
