package main

import (
	"context"

	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func ReadDataAndSendDirectly(client *influxdb2.Client, start string, end string, eqpId string, producer *kafka.Producer) (int, float64) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)
	query := `
    from(bucket: "hobit_iot_sensor")
    |> range(start: ` + start + `, stop: ` + end + `)
    |> filter(fn: (r) => r["eqp_id"] == "` + eqpId + `")
    `
	startTime := time.Now()
	results, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(startTime)
	fmt.Printf("client: %x Query took %s\n", client, elapsed)

	totalProcessed := 0
	// loopStartTime := time.Now() // 루프 시작 시간
	for results.Next() {
		record := results.Record()
		if v, ok := record.Values()["_value"].(string); ok {
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(v),
			}, nil)
			totalProcessed++
		}
	}
	// loopElapsed := time.Since(loopStartTime) // 루프 처리 시간
	if err := results.Err(); err != nil {
		log.Fatal(err)
	}

	// 초당 처리 레코드 수 계산
	recordsPerSecond := 0.0
	// if loopElapsed.Seconds() > 0 {
	// 	recordsPerSecond = float64(totalProcessed) / loopElapsed.Seconds()
	// }

	return totalProcessed, recordsPerSecond
}
