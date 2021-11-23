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

func (l *Log) LastTest() int64 {
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

func (l *Log) Append(term int64, command Command) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := NewEntry(term, l.lastIndex+1, command)
	return l.append(e)
}

func (l *Log) Entry(index int64) (Entry, bool) {
	return l.getEntry(index)
}

func (l *Log) append(entry Entry) bool {
	if entry.index <= l.lastIndex {
		if term, ok := l.getTerm(entry.index); ok {
			if term != entry.term {
				l.wipeConflictedEntries(entry.index)
			} else {
				return true
			}
		}
		if entry.index == l.lastIndex+1 && entry.term >= l.lastTerm {
			l.entries = append(l.entries, entry)

			if l.firstIndex == 0 {
				l.firstIndex = entry.index
				l.firstTerm = entry.term
				log.Infof("[%d] log setting first index = %d (%d)", l.id, l.firstIndex, entry.index)
			}
			l.lastIndex = entry.index
			l.lastTerm = entry.term
			return true
		}

	}
	return false
}

func (l *Log) getTerm(index int64) (int64, bool) {
	if e, ok := l.getEntry(index); ok {
		return e.term, true
	}
	return -1, false
}

func (l *Log) getEntry(index int64) (Entry, bool) {
	if index > 0 && index <= l.lastIndex {
		if index > l.firstIndex && len(l.entries) > 0 {
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
