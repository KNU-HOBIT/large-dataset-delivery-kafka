package main

import (
	"fmt"
)

type Dispatcher struct {
	WorkerPool chan chan Job
	MaxWorkers int
	Workers    []Worker
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool, MaxWorkers: maxWorkers}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, i+1)
		d.Workers = append(d.Workers, worker) // 워커를 슬라이스에 추가
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	fmt.Println("dispatch start")
	// Use for range to automatically listen to channel until it's closed
	for job := range JobQueue {
		go func(job Job) {
			// Retrieve a worker's job channel from the pool
			jobChannel := <-d.WorkerPool
			// Send the job to the retrieved worker
			jobChannel <- job
		}(job)
	}
}

func (d *Dispatcher) StopAllWorkers() {
	for _, worker := range d.Workers {
		worker.Stop() // 각 워커에 대해 Stop 메소드 호출
	}
}
