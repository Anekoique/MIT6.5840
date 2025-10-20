package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true

func init() {
	// Disable the default date/time prefix added by the standard logger.
	log.SetFlags(0)
}

func DPrintf(format string, a ...any) {
	if Debug {
		timestamp := time.Now().Format("15:04:05.000")
		args := append([]any{timestamp}, a...)
		log.Printf("%s "+format, args...)
	}
}
