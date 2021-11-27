package raft

import "os"

func Min(a, b int64) int64 {
	if a <= b {
		return a
	}
	return b
}

func Max(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func FileExist(filePath string) bool {

	if _, err := os.Stat(filePath); err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}
