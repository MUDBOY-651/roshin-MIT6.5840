package raft

import (
	"fmt"
	"log"
)

// Debugging
//const Debug = true

const Debug = false

var void interface{}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func Dprintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

//const DEBUG_TEST = true

const DEBUG_TEST = false

func ImportantInfo(format string, a ...interface{}) {
	if DEBUG_TEST {
		fmt.Printf("---------------------------------------------------------------\n")
		log.Printf(format, a...)
		fmt.Printf("---------------------------------------------------------------\n")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
