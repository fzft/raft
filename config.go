package raft

type Config struct {
	logDir string
}

func NewConfig() Config {
	return Config{
		logDir: "/Users/fangzhenfutao/go/src/github.com/fzft/raft",
	}
}
