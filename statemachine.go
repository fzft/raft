package raft

import (
	"fmt"
	"github.com/fzft/raft/storage"
	"github.com/tinylib/msgp/msgp"
	"io"
	"sync"
)

type CommandId int64

const (
	Snapshot_File_Veriosn      int = 1
	Storage_State_File_Version int = 1

	CommandIdTestCommand CommandId = iota
	CommandIdNewTerm
	CommandIdHealthCheck
)

type Srv struct {
	id   int64
	host string
	port int64
}

func NewSrv(id int64, host string, port int64) Srv {
	return Srv{
		id:   id,
		host: host,
		port: port,
	}
}

func NewSrvRead(r *msgp.Reader) Srv {
	s := Srv{}
	s.id, _ = r.ReadInt64()
	s.host, _ = r.ReadString()
	s.port, _ = r.ReadInt64()
	return s
}

func (s *Srv) write(w *msgp.Writer) {
	w.WriteInt64(s.id)
	w.WriteString(s.host)
	w.WriteInt64(s.port)
}

type StateMachine struct {
	id int64
	mu sync.Mutex

	// state
	index            int64
	term             int64
	checksum         int64
	count            int64
	prevIndex        int64
	prevTerm         int64
	severs           map[int64]Srv
	commandFactories map[CommandId]Command
	copyOnWrite      map[int64]map[string]*storage.Item
	items            map[string]*storage.Item

	lastCommandAppliedMills int64
}

func NewStateMachine(id int64) *StateMachine {
	return &StateMachine{
		id: id,
	}
}

func (s *StateMachine) saveState(mw *msgp.Writer) {
	mw.WriteInt(Storage_State_File_Version)
	mw.WriteInt(len(s.items))
	for _, i := range s.items {
		i.Write(mw)
	}
}

func (s *StateMachine) loadState(mr *msgp.Reader) error {
	var err error
	s.items = make(map[string]*storage.Item)
	s.copyOnWrite = make(map[int64]map[string]*storage.Item)
	fileVersion, err := mr.ReadInt()
	if fileVersion > Storage_State_File_Version {
		return fmt.Errorf("Incompatible Snapshot Format: %d > %d", fileVersion, Storage_State_File_Version)
	}
	numItems, err := mr.ReadInt()
	for i := 0; i < numItems; i++ {
		var item *storage.Item
		item, err = storage.NewItemRead(mr, fileVersion)
		s.items[item.Key] = item
	}
	return err
}

func (s *StateMachine) writeSnapshot(w io.Writer, prevTerm int64) {
	mw := msgp.NewWriter(w)
	mw.WriteInt(Snapshot_File_Veriosn)
	mw.WriteInt64(s.term)
	mw.WriteInt64(s.index)
	mw.WriteInt64(prevTerm)
	mw.WriteInt64(s.count)
	mw.WriteInt64(s.checksum)
	mw.WriteInt(len(s.severs))
	for _, srv := range s.severs {
		srv.write(mw)
	}
	s.saveState(mw)
}

func (s *StateMachine) readSnapshot(r io.Reader) (err error) {
	mr := msgp.NewReader(r)
	fileVersion, err := mr.ReadInt()
	if fileVersion > Snapshot_File_Veriosn {
		return fmt.Errorf("[%d] state machine Incompatible Snapshot Format:  fileVersion %d > %d ", s.id, fileVersion, Snapshot_File_Veriosn)
	}
	s.term, err = mr.ReadInt64()
	s.index, err = mr.ReadInt64()
	s.prevIndex = s.index - 1
	s.prevTerm, err = mr.ReadInt64()
	s.count, err = mr.ReadInt64()
	s.checksum, err = mr.ReadInt64()
	s.severs = make(map[int64]Srv)
	numServes, err := mr.ReadInt()
	for i := 0; i < numServes; i++ {
		s.severs[int64(i)] = NewSrvRead(mr)
	}
	s.loadState(mr)
	return
}

func (s *StateMachine) getIndex() int64 {
	return s.index
}

func (s *StateMachine) getTerm() int64 {
	return s.term
}

func GetSnapshotIndex(r io.Reader) (int64, error) {
	mr := msgp.NewReader(r)
	fileVersion, err := mr.ReadInt()
	if err != nil {
		return -1, err
	}
	if fileVersion > Snapshot_File_Veriosn {
		return -1, fmt.Errorf("State machine Incompatible Snapshot Format:  fileVersion %d > %d ", fileVersion, Snapshot_File_Veriosn)
	}
	_, _ = mr.ReadInt64()
	index, err := mr.ReadInt64()
	return index, err
}
