package fanout

import (
	"context"
)

type FanOut[T any] struct {
	in chan T
}

func NewFanOut[T any](in chan T) *FanOut[T] {
	return &FanOut[T]{
		in: in,
	}
}

func (o *FanOut[T]) Split(cnt context.Context, count int) []chan T {
	out := make([]chan T, count)
	for i := range count {
		out[i] = make(chan T)
	}

	go func() {
		defer func() {
			for i := range count {
				close(out[i])
			}
		}()

		i := 0
		for {
			select {
			case <-cnt.Done():
				return
			case v, ok := <-o.in:
				if !ok {
					return
				}
				out[i] <- v
				i = (i + 1) % count
			}
		}
	}()

	return out
}
