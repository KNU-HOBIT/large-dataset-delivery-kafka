package main

import (
	"fmt"
	"sync"
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

func (d *Dispatcher) Run(wg *sync.WaitGroup) {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, i+1, wg)
		d.Workers = append(d.Workers, worker) // 워커를 슬라이스에 추가
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	fmt.Println("dispatch start")
	for {
		select {
		case job := <-JobQueue:
			// JobQueue에서 작업을 받아옴
			go func(job Job) {
				jobChannel := <-d.WorkerPool
				// 작업을 워커에게 전달
				jobChannel <- job
			}(job)
		}
	}
}

func (d *Dispatcher) StopAllWorkers() {
	for _, worker := range d.Workers {
		worker.Stop() // 각 워커에 대해 Stop 메소드 호출
	}
}
