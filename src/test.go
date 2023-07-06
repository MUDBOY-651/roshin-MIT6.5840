package main

import "fmt"
import "time"

var ch chan int

func Hello() {
  go func() {
    x := 1
    ch<-x
    fmt.Println("Hello")
  }()
}
func f() {
  t := <-ch
  fmt.Printf("t=%d\n", t)
}

func main() {
  ch = make(chan int, 1)
  Hello()
  time.Sleep(time.Second)
  f()
}
