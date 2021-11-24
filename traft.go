package raft

import (
	log "github.com/sirupsen/logrus"
	"testing"
	"time"
)

type TRaft struct {
	n         int
	cluster   []*Server
	readyCh   chan struct{}
	connected []bool
	t         *testing.T
}

func NewTRaft(n int, t *testing.T) *TRaft {
	readyCh := make(chan struct{})
	ns := make([]*Server, n)
	connected := make([]bool, n)

	for i := 0; i < n; i++ {
		peerIds := make([]int64, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, int64(p))
			}
		}

		ns[i] = NewServer(int64(i), peerIds, readyCh)
		ns[i].Serve()
	}

	// Connect all peers to each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(int64(j), ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}

	close(readyCh)

	r := &TRaft{
		t:         t,
		n:         n,
		cluster:   ns,
		readyCh:   readyCh,
		connected: connected,
	}
	return r
}

func (r *TRaft) Shutdown() {

	for i := 0; i < r.n; i++ {
		r.cluster[i].DisconnectAll()
	}
	for i := 0; i < r.n; i++ {
		r.cluster[i].Shutdown()
	}

}

func (r *TRaft) CheckSingleLeader() (int, int64) {
	for k := 0; k < 8; k++ {
		leaderId := -1
		leaderTerm := int64(-1)
		for i := 0; i < r.n; i++ {
			if r.connected[i] {
				_, term, isLeader := r.cluster[i].raft.report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						log.Infof("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	log.Infof("leader not found")
	return -1, -1
}

func (r *TRaft) SubmitToServer(leaderId int, command TestCommand) bool {
	return r.cluster[leaderId].raft.executeCommand(command)
}
