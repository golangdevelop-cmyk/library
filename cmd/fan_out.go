package main

import (
	"context"
	"sync"
	"time"

	"github.com/golangdevelop-cmyk/library/internal/concurrent/fanout"
)

func main() {
	data := make(chan int)
	cnt, cns := context.WithTimeout(context.Background(), time.Second*10)
	defer cns()

	go func() {
		defer close(data)
		for v := range 10 {
			data <- v
		}
	}()

	fo := fanout.NewFanOut(data)
	channels := fo.Split(cnt, 2)

	wg := sync.WaitGroup{}
	wg.Add(len(channels))

	go func() {
		defer wg.Done()
		for v := range channels[0] {
			println("1->", v)
		}
	}()

	go func() {
		defer wg.Done()
		for v := range channels[1] {
			println("2->", v)
		}
	}()

	wg.Wait()
}
