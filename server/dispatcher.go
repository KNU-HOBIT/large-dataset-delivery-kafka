package main

import (
	"fmt"
	"sync"
)

type Dispatcher struct {
	JobQueue   chan Job
	WorkerPool chan chan Job
	MaxWorkers int
	Workers    []Worker
	wg         *sync.WaitGroup
	stopped    bool
	mu         sync.Mutex
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	JobQueue := make(chan Job, config.Jobs.JobQueueCapacity)
	wg := sync.WaitGroup{}
	return &Dispatcher{
		JobQueue:   JobQueue,
		WorkerPool: pool,
		MaxWorkers: maxWorkers,
		wg:         &wg,
	}
}

func (d *Dispatcher) Run(connectionID string) {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, i+1, d, connectionID)
		d.Workers = append(d.Workers, worker)
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	fmt.Println("dispatch start")
	for job := range d.JobQueue {
		go func(job Job) {
			jobChannel := <-d.WorkerPool
			jobChannel <- job
		}(job)
	}
}

func (d *Dispatcher) SubmitJob(job *Job) {
	d.wg.Add(1)
	d.JobQueue <- *job
}

func (d *Dispatcher) WaitForAllJobs() {
	d.wg.Wait()
}

func (d *Dispatcher) StopAllWorkers() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return // 이미 중지됨
	}

	for _, worker := range d.Workers {
		worker.Stop()
	}

	close(d.WorkerPool)
	close(d.JobQueue)
	d.Workers = nil
	d.stopped = true

	fmt.Println("All workers have been stopped, and resources are cleaned up.")
}
