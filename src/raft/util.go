package raft

import (
	"fmt"
	"log"
  "runtime"
)

// Debugging
//const Debug = false

const Debug = true

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

func dbg(format string, a ...interface{}) (n int, err error) {
  if Debug {
		fmt.Printf("---------------------------------------------------------------\n")
		fmt.Printf(format, a...)
		fmt.Printf("---------------------------------------------------------------\n")
  }
  return 
}

func PrintLockInfo (v interface{}) {
	_, _, line, _ := runtime.Caller(1)
  if Debug {
    fmt.Printf("[LockInfo] LINE:%d Try Lock\n", line)
  }
}

//const DEBUG_TEST = true

const DEBUG_TEST = true


func ImportantInfo(format string, a ...interface{}) {
	if DEBUG_TEST {
		fmt.Printf("---------------------------------------------------------------\n")
		fmt.Printf(format, a...)
		fmt.Printf("---------------------------------------------------------------\n")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
