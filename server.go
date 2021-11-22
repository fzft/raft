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
	readyCh     chan struct{}
	stopCh      chan struct{}
}

func NewServer(id int64, peersIds []int64, readyCh chan struct{}) *Server {
	return &Server{
		id:          id,
		peerIds:     peersIds,
		peerClients: make(map[int64]*rpc.Client),
		readyCh:     readyCh,
		stopCh:      make(chan struct{}),
	}
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.raft = NewEngine(s.id, s, s.peerIds, s.readyCh)
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

func (s *Server) Shutdown() {
	s.raft.stop()
	s.listener.Close()
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int64, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}
