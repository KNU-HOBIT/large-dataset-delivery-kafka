package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Worker struct {
	Dispatcher
	ID           int
	WorkerPool   chan chan Job
	JobChannel   chan Job
	quit         chan bool
	producer     *kafka.Producer
	connectionID string         // 이 워커가 담당할 connectionID
	dbClient     DatabaseClient // 이 워커 전용 DB 클라이언트
}

func NewWorker(workerPool chan chan Job, id int, d *Dispatcher, connectionID string) Worker {
	// Kafka producer 초기화
	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers":  config.Kafka.BootstrapServers,
			"acks":               config.Kafka.Acks,
			"enable.idempotence": config.Kafka.EnableIdempotence,
			"compression.type":   config.Kafka.CompressionType,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to create Kafka producer for worker %d: %v", id, err))
	}

	// connectionManager에서 연결 정보 가져오기
	connInfo, err := connectionManager.GetConnectionInfo(connectionID)
	if err != nil {
		panic(fmt.Sprintf("Failed to get connection info '%s' for worker %d: %v", connectionID, id, err))
	}

	// 연결 정보를 바탕으로 새로운 DatabaseClient 생성 (handleDBConnectionRegister와 같은 로직)
	var dbClient DatabaseClient
	switch connInfo.DBType {
	case "influx":
		dbClient, err = NewInfluxDBClient(connInfo.URL, connInfo.Token, connInfo.Org)
		if err != nil {
			panic(fmt.Sprintf("Failed to create InfluxDB client for worker %d: %v", id, err))
		}
	case "mongo":
		dbClient, err = NewMongoDBClient(connInfo.URL, connInfo.Database)
		if err != nil {
			panic(fmt.Sprintf("Failed to create MongoDB client for worker %d: %v", id, err))
		}
	default:
		panic(fmt.Sprintf("Unsupported database type '%s' for worker %d", connInfo.DBType, id))
	}

	return Worker{
		Dispatcher:   *d,
		ID:           id,
		WorkerPool:   workerPool,
		JobChannel:   make(chan Job),
		quit:         make(chan bool),
		producer:     producer,
		connectionID: connectionID,
		dbClient:     dbClient,
	}
}

func (w *Worker) handleDeliveryReports() {
	for {
		select {
		case e := <-w.producer.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}

		case <-w.quit:
			fmt.Printf("worker %d stop\n", w.ID)
			fmt.Println("Stopping handleDeliveryReports goroutine...")
			return
		}
	}
}

func (w *Worker) Start() {
	go func() {
		fmt.Printf("worker %d start (connectionID: %s)\n", w.ID, w.connectionID)
		defer w.producer.Close()
		defer w.dbClient.Close()

		// Kafka Producer의 이벤트를 처리하는 고루틴을 시작
		go w.handleDeliveryReports()

		for {
			// 현재 워커를 사용 가능한 워커 풀에 추가
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				flushEntered := false
				var flushStartTime time.Time
				var startTime time.Time = time.Now()
				if job.params.DBType == "influx" {
					fmt.Printf("worker%d: job start about: %s~%s\n", w.ID, job.execInfo.StartStr, job.execInfo.EndStr)
				} else if job.params.DBType == "mongo" {
					fmt.Printf("worker%d: job start about: %d~%d\n", w.ID, job.execInfo.StartOffset, job.execInfo.EndOffset)
				}

				var totalProcessed int = 0
				totalProcessed = w.ReadDataAndSendDirectly(&job)

				// Kafka flush 처리
				unflushed := w.producer.Flush(config.Kafka.FlushTimeoutMs)
				for unflushed > 0 {
					if !flushEntered {
						flushStartTime = time.Now()
						flushEntered = true
					}
					fmt.Printf("worker%d: unflushed %d msg.\n", w.ID, unflushed)
					unflushed = w.producer.Flush(config.Kafka.FlushTimeoutMs)
				}

				var endTime time.Time
				if flushEntered {
					endTime = time.Now()
					fmt.Printf("worker%d: flush duration %v\n", w.ID, endTime.Sub(flushStartTime))
				} else {
					endTime = time.Now()
				}
				fmt.Printf("worker %d produced records count: %d job completed in %v\n", w.ID, totalProcessed, endTime.Sub(startTime))
				job.messagesCh <- totalProcessed
				w.Dispatcher.wg.Done() // Dispatcher의 WaitGroup도 완료 표시

			case <-w.quit:
				w.producer.Flush(config.Kafka.FlushTimeoutMs)
				fmt.Printf("worker %d end\n", w.ID)
				return
			}
		}
	}()
}

func (w *Worker) ReadDataAndSendDirectly(job *Job) int {

	// 데이터베이스에서 데이터 읽고 Kafka로 전송
	startTime := time.Now()
	totalProcessed, err := w.dbClient.ReadDataAndSend(
		job.params,
		job.execInfo, // JobExecutionInfo 전달
		w.producer,
		job.execInfo.SendTopic,
		job.jobList.partitionCount,
	)
	if err != nil {
		log.Printf("Error reading data and sending to Kafka: %v", err)
		return 0
	}
	elapsed := time.Since(startTime)
	fmt.Printf("Database query and Kafka send took %s\n", elapsed)

	return totalProcessed
}

func (w *Worker) Stop() {
	w.quit <- true
}
