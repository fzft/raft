package raft

type Command interface {
	Name() string
}

type TestCommand struct {
	Name string
}

func NewTestCommand(name string) TestCommand {
	return TestCommand{
		Name: name,
	}
}


type HealthCheckCommand struct {
}
