package workerpool

import (
	"context"
	"sync"
)

type Result[T any] struct {
	workerId int
	job      T
	err      error
}

func (o *Result[T]) Error() error {
	return o.err
}
func (o *Result[T]) Job() T {
	return o.job
}
func (o *Result[T]) NewResult(job T, err error) *Result[T] {
	return &Result[T]{job: job, err: err}
}

type ContextDoneError struct {
	Message string
}

func (e *ContextDoneError) Error() string {
	return e.Message
}

type ChannelClosedError struct {
	Message string
}

func (e *ChannelClosedError) Error() string {
	return e.Message
}

type WorkerPool[T any] struct {
	wg      *sync.WaitGroup
	context context.Context
	size    int
	jobs    <-chan T
	action  func(job T) error
}

func NewWorkerPool[T any](size int, context context.Context, jobs <-chan T, action func(job T) error) *WorkerPool[T] {
	return &WorkerPool[T]{
		size:    size,
		wg:      &sync.WaitGroup{},
		context: context,
		jobs:    jobs,
		action:  action,
	}
}

func (o *WorkerPool[T]) Execute() <-chan Result[T] {
	results := make(chan Result[T])

	for i := range o.size {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			worker(i,
				o.context,
				o.jobs,
				o.action,
				results)
		}()
	}

	go func() {
		o.wg.Wait()
		close(results)
	}()
	return results
}

func worker[T any](id int, context context.Context, jobs <-chan T, action func(T) error, out chan<- Result[T]) {
	for {
		select {
		case <-context.Done():
			out <- Result[T]{
				workerId: id,
				err:      &ContextDoneError{Message: "Context done"},
			}
			return
		case job, ok := <-jobs:
			if !ok {
				out <- Result[T]{
					workerId: id,
					job:      job,
					err:      &ChannelClosedError{Message: "Channel closed"},
				}
			}
			out <- Result[T]{
				workerId: id,
				job:      job,
				err:      action(job),
			}
		}
	}
}
