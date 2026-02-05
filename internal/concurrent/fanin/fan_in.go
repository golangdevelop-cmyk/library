package fanin

import (
	"context"
	"sync"
)

type FanIn[T any] struct {
	in []chan T
}

func NewFanIn[T any](in ...chan T) *FanIn[T] {
	return &FanIn[T]{
		in: in,
	}
}

func (o *FanIn[T]) Join(cnt context.Context) chan T {
	out := make(chan T)

	wg := &sync.WaitGroup{}
	wg.Add(len(o.in))

	for i := range len(o.in) {
		go func() {
			for {
				select {
				case <-cnt.Done():
					wg.Done()
					return
				case v, ok := <-o.in[i]:
					if !ok {
						wg.Done()
						return
					}
					out <- v
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
