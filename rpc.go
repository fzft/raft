package raft

type Rpc interface {
	HandleVoteRequest()
	SendRequestVote(peerId, term, candidateId int64, handler func(term int64, voteGranted bool))
	SendAppendEntries(peerId, term int64, handler func(term int64, success bool))
}

type EngineRpc struct {
	raft *Engine
}

func NewEngineRpc(r *Engine) *EngineRpc {
	return &EngineRpc{
		raft: r,
	}
}


func (rpc *EngineRpc) SendRequestVote(peerId, term, candidateId int64, handler func(term int64, voteGranted bool)) {



}