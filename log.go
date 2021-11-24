package raft

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

// Log ...
type Log struct {
	id          int64
	mu          sync.Mutex
	entries     []Entry
	firstIndex  int64
	firstTerm   int64
	lastIndex   int64
	lastTerm    int64
	commitIndex int64
}

func NewLog(id int64) *Log {
	return &Log{
		id: id,
	}
}

func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(len(l.entries))
}

func (l *Log) LastIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndex
}

func (l *Log) FirstIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.firstIndex
}

func (l *Log) FirstTerm() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.firstTerm
}

func (l *Log) LastTerm() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastTerm
}

func (l *Log) CommitIndex() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.commitIndex
}

func (l *Log) SetCommitIndex(index int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commitIndex = index
}

func (l *Log) GetEntries(fromIndex int64) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entries[fromIndex:]
}

func (l *Log) IsConsistentWith(index, term int64) bool {
	if (index == 0 && term == 0) || index > l.lastIndex {
		return true
	}
	if e, ok := l.getEntry(index); ok {
		return e.Term == term
	}
	return false
}

func (l *Log) Append(term int64, command TestCommand) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := NewEntry(term, l.lastIndex+1, command)
	return l.append(e)
}

func (l *Log) Entry(index int64) (Entry, bool) {
	return l.getEntry(index)
}

func (l *Log) append(entry Entry) bool {
	if entry.Index <= l.lastIndex {
		if term, ok := l.getTerm(entry.Index); ok {
			if term != entry.Term {
				l.wipeConflictedEntries(entry.Index)
			} else {
				return true
			}
		}
	}
	if entry.Index == l.lastIndex+1 && entry.Term >= l.lastTerm {
		l.entries = append(l.entries, entry)

		if l.firstIndex == 0 {
			l.firstIndex = entry.Index
			l.firstTerm = entry.Term
			log.Infof("[%d] log setting first index = %d term= %d (%d)", l.id, l.firstIndex, l.firstTerm, entry.Index)
		}
		l.lastIndex = entry.Index
		l.lastTerm = entry.Term
		log.Infof("[%d] log setting index= %d term= %d ", l.id, entry.Index, entry.Term)
		return true
	}
	return false
}

func (l *Log) getTerm(index int64) (int64, bool) {
	if index == 0 {
		return 0, true
	}
	if e, ok := l.getEntry(index); ok {
		return e.Term, true
	}
	return -1, false
}

func (l *Log) getEntry(index int64) (Entry, bool) {
	if index > 0 && index <= l.lastIndex {
		if index >= l.firstIndex && len(l.entries) > 0 {
			e := l.entries[index-l.firstIndex]
			return e, true
		} else {
			return l.getEntryFromDisk(index)
		}
	}
	return Entry{}, false
}

func (l *Log) wipeConflictedEntries(index int64) {
	// TODO conflict
}

func (l *Log) getEntryFromDisk(index int64) (Entry, bool) {
	// TODO getEntryFromDisk
	return Entry{}, false
}
