package raft

import (
	"testing"
	"time"
)

func TestEngine(t *testing.T) {
	n := 3
	readyCh := make(chan struct{})
	ns := make([]*Server, n)

	defer func() {
		for i := 0; i < n; i++ {
			ns[i].DisconnectAll()
		}
		for i := 0; i < n; i++ {
			ns[i].Shutdown()
		}
	}()

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
	}

	close(readyCh)

	time.Sleep(150 * time.Second)
}
