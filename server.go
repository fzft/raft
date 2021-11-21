package raft

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	id   int64
	mu   sync.Mutex
	raft *Engine

	peerIds     []int64
	rpcServer   *rpc.Server
	peerClients map[int64]*rpc.Client
	listener    net.Listener
	stopCh      chan struct{}
}

func NewServer(id int64, peersIds []int64) *Server {
	return &Server{
		id:          id,
		peerIds:     peersIds,
		peerClients: make(map[int64]*rpc.Client),
		stopCh:      make(chan struct{}),
	}
}

func (s *Server) Serve() {
	s.raft = NewEngine(s.id, s)
	s.rpcServer = rpc.NewServer()
	s.rpcServer.RegisterName("Engine", s.raft)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.id, s.listener.Addr())
	s.mu.Unlock()

	go func() {

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.stopCh:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			go func() {
				s.rpcServer.ServeConn(conn)
			}()
		}
	}()
}

func (s *Server) Call(id int64, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}
