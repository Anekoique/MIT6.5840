package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}
