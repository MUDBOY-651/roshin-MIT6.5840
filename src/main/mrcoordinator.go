package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.5840/mr"
	"io/ioutil"
	"log"
)
import "time"
import "os"
import "fmt"

func main() {
	log.SetOutput(ioutil.Discard)
	if len(os.Args) < 2 {
		_, err := fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		if err != nil {
			return
		}
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
