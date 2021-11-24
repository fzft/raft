package raft

type Entry struct {
	Term    int64
	Index   int64
	Command TestCommand
}

func NewEntry(term, index int64, command TestCommand) Entry {
	return Entry{
		Term:    term,
		Index:   index,
		Command: command,
	}
}

func (e Entry) getTerm() int64 {
	return e.Term
}

func (e Entry) getIndex() int64 {
	return e.Index
}

func (e Entry) getCommand() TestCommand {
	return e.Command
}


