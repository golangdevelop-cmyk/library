package main

import (
	"context"
	"fmt"
	"time"

	"github.com/golangdevelop-cmyk/library/internal/concurrent/fanin"
)

func main() {
	data1 := make(chan int)
	data2 := make(chan int)

	cnt, cns := context.WithTimeout(context.Background(), time.Second*10)
	defer cns()

	go func() {
		defer close(data1)
		for v := range 5 {
			data1 <- v
		}
	}()

	go func() {
		defer close(data2)
		for v := range 5 {
			data2 <- v
		}
	}()

	fo := fanin.NewFanIn(data1, data2)
	channel := fo.Join(cnt)

	for v := range channel {
		fmt.Println(v)
	}
}
