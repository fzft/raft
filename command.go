package raft

type Command interface {
	ApplyTo()
	Write()
	Read()
	CommandType() CommandId
}

type TestCommand struct {
	Name string
}

func NewTestCommand(name string) TestCommand {
	return TestCommand{
		Name: name,
	}
}

func (c TestCommand) ApplyTo() {

}

func (c TestCommand) Write() {

}
func (c TestCommand) CommandType() CommandId {
	return CommandIdTestCommand
}
func (c TestCommand) Read() {

}

type TermCommand struct {
	Term   int64
	PeerId int64
}

func NewTermCommand(term, peerId int64) TermCommand {
	return TermCommand{
		Term:   term,
		PeerId: peerId,
	}
}

func (c TermCommand) ApplyTo() {

}

func (c TermCommand) Write() {

}
func (c TermCommand) CommandType() CommandId {
	return CommandIdNewTerm
}
func (c TermCommand) Read() {

}

type HealthCheckCommand struct {
}
