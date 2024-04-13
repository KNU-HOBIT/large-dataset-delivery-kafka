package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
)

type Worker struct {
	ID         int
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
	wg         *sync.WaitGroup
}

func NewWorker(workerPool chan chan Job, id int, wg *sync.WaitGroup) Worker {
	return Worker{
		ID:         id,
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
		wg:         wg,
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
				} else {
					// fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
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
				"bootstrap.servers":            "155.230.34.51:32100,155.230.34.52:32100,155.230.34.53:32100",
				"acks":                         "all", // 모든 브로커의 확인을 받아야 성공으로 간주
				"retries":                      20,    // 메시지 전송 실패 시 재시도 횟수
				"queue.buffering.max.kbytes":   8457280,
				"queue.buffering.max.messages": 1000000,
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

				fmt.Printf("worker%d: job start about: %s~%s\n", w.ID, job.startStr, job.endStr)
				records := ReadData(&client, job.startStr, job.endStr, job.eqpId)
				// fmt.Printf("worker %d read records count: %d\n", w.ID, len(records))

				// // `records` 리스트가 비어있지 않은 경우, 첫 번째 요소 출력
				// if len(records) > 0 {
				// 	fmt.Printf("첫 번째 record 요소: %+v\n", records[0])
				// } else {
				// 	fmt.Println("record 리스트가 비어있습니다.")
				// }

				var messagesProcessed int = 0
				for _, record := range records {
					// 각 record를 JSON으로 직렬화
					jsonRecord, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
					if err != nil {
						fmt.Printf("JSON 데이터를 문자열로 직렬화하는 중 에러 발생: %v", err)
						continue // 에러가 발생한 경우, 다음 record 처리로 넘어감
					}

					// // 최초 반복에서만 특정 작업을 수행
					// if i == 0 {
					// 	fmt.Printf("jsonRecord의 첫 부분: %s\n", string(jsonRecord)[:min(100, len(jsonRecord))])
					// }

					// Kafka 메시지 생성 및 전송
					err = producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          jsonRecord,
					}, nil)
					if err != nil {
						fmt.Printf("Error producing message: %s\n", err)

					}

					messagesProcessed++
				}
				// Ensure the delivery report handler has finished
				unflushed := producer.Flush(15 * 1000) // 15 seconds
				if unflushed > 0 {
					fmt.Printf("unflushed %d msg.\n", unflushed)
				}

				fmt.Printf("worker %d produced records count: %d\n", w.ID, messagesProcessed)
				*job.messagesCh <- messagesProcessed
				w.wg.Done() // 작업 처리 완료를 알림

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
