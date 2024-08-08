package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Worker struct {
	ID         int
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job, id int) Worker {
	return Worker{
		ID:         id,
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

func (w *Worker) handleDeliveryReports(producer *kafka.Producer) {
	for {
		select {
		case e := <-producer.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				}
				// else {
				// 	// fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				// }
			}

		case <-w.quit:
			fmt.Println("Stopping handleDeliveryReports goroutine...")
			return // 고루틴 종료
		}
	}
}

func (w *Worker) Start() {

	go func() {
		fmt.Printf("worker %d start\n", w.ID)
		//INIT influx client
		client := influxdb2.NewClientWithOptions(url, token,
			influxdb2.DefaultOptions().
				SetPrecision(time.Millisecond).
				SetHTTPRequestTimeout(900))
		//INIT kafka producer
		producer, err := kafka.NewProducer(
			&kafka.ConfigMap{
				"bootstrap.servers":  config.Kafka.BootstrapServers,
				"acks":               config.Kafka.Acks,
				"enable.idempotence": config.Kafka.EnableIdempotence,
				"compression.type":   config.Kafka.CompressionType,
				// "debug":              "msg",
				// "linger.ms":          500,
				// "batch.size":         5000000,
				// "queue.buffering.max.kbytes":   MaxKBytes,
				// "queue.buffering.max.messages": MaxMessages,

			})
		if err != nil {
			panic(err)
		}
		defer client.Close()
		defer producer.Close()

		// Kafka Producer의 이벤트를 처리하는 고루틴을 시작합니다.
		go w.handleDeliveryReports(producer)

		for {
			// 현재 워커를 사용 가능한 워커 풀에 추가
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:

				// 여기서 실제 작업을 처리
				// 예제: job의 데이터를 출력
				flushEntered := false                // flush 작업 진입 여부
				var flushStartTime time.Time         // flush 작업 시작 시간
				var startTime time.Time = time.Now() // 작업 시작 시간 기록
				fmt.Printf("worker%d: job start about: %s~%s\n", w.ID, job.startStr, job.endStr)

				var totalProcessed int = 0
				totalProcessed, recordsPerSecond := ReadDataAndSendDirectly(&client, &job, producer)

				// Ensure the delivery report handler has finished
				unflushed := producer.Flush(15 * 1000) // 15 seconds
				for unflushed > 0 {
					if !flushEntered {
						flushStartTime = time.Now() // 첫 flush 작업 시작 시간 기록
						flushEntered = true
					}
					fmt.Printf("worker%d: unflushed %d msg.\n", w.ID, unflushed)
					unflushed = producer.Flush(15 * 1000) // 15 seconds
				}
				var endTime time.Time
				if flushEntered {
					endTime = time.Now() // flush 진입이 있었을 경우, flush 완료 시간을 종료 시간으로 기록
					fmt.Printf("worker%d: flush duration %v\n", w.ID, endTime.Sub(flushStartTime))
				} else {
					endTime = time.Now() // flush 진입이 없었을 경우, 현재 시간을 종료 시간으로 기록
				}
				fmt.Printf("worker %d produced records count: %d job completed in %v\n", w.ID, totalProcessed, endTime.Sub(startTime))
				fmt.Printf("worker %d processed Records/second: %.2f\n", w.ID, recordsPerSecond)
				job.messagesCh <- totalProcessed
				job.wg.Done() // 작업 처리 완료를 알림

			case <-w.quit:
				// 워커 종료
				client.Close()
				fmt.Printf("worker %d end\n", w.ID)
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.quit <- true
}
