package raft

import (
	log "github.com/sirupsen/logrus"
	"testing"
	"time"
)

func TestEngine(t *testing.T) {
	r := NewTRaft(3, t)
	defer r.Shutdown()
	r.CheckSingleLeader()
	time.Sleep(1500 * time.Millisecond)
}

func TestCommitOne(t *testing.T) {
	r := NewTRaft(3, t)
	defer r.Shutdown()
	origLeaderId, _ := r.CheckSingleLeader()

	log.Infof("submitting 'foo' command to %d", origLeaderId)
	success := r.SubmitToServer(origLeaderId, NewTestCommand("foo"))
	if !success {
		t.Errorf("want id=%d leader, but it's not", origLeaderId)
	}
	time.Sleep(1500 * time.Millisecond)
}

func TestCommitMany(t *testing.T) {
	r := NewTRaft(3, t)
	defer r.Shutdown()
	origLeaderId, _ := r.CheckSingleLeader()

	log.Infof("submitting commands to %d", origLeaderId)
	r.SubmitToServer(origLeaderId, NewTestCommand("foo"))
	r.SubmitToServer(origLeaderId, NewTestCommand("spam"))
	r.SubmitToServer(origLeaderId, NewTestCommand("hello"))
	r.SubmitToServer(origLeaderId, NewTestCommand("world"))
	time.Sleep(5000 * time.Millisecond)
}
