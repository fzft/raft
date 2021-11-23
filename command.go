package raft

type Command interface {
	ApplyTo()
	Write()
	Read()
	CommandType() int
}


type HealthCheckCommand struct {
}

