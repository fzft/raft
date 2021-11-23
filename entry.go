package raft

type Entry struct {
	term    int64
	index   int64
	command Command
}

func NewEntry(term, index int64, command Command) Entry {
	return Entry{
		term:    term,
		index:   index,
		command: command,
	}
}

func (e Entry) getTerm() int64 {
	return e.term
}

func (e Entry) getIndex() int64 {
	return e.index
}

func (e Entry) getCommand() Command {
	return e.command
}


