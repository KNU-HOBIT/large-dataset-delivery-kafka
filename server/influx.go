package main

import (
	"context"

	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func CheckStartEndRange(client *influxdb2.Client, bucket, eqpId string) (startTsStr, endTsStr string) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)

	// Define the query for getting the first timestamp
	firstQuery := `
	from(bucket: "` + bucket + `")
		|> range(start: 0)  // Query all data
		|> filter(fn: (r) => r["eqp_id"] == "` + eqpId + `")
		|> first()
	`

	// Define the query for getting the last timestamp
	lastQuery := `
	from(bucket: "` + bucket + `")
		|> range(start: 0)  // Query all data
		|> filter(fn: (r) => r["eqp_id"] == "` + eqpId + `")
		|> last()
	`

	// Execute the first query
	firstResults, err := queryAPI.Query(context.Background(), firstQuery)
	if err != nil {
		log.Fatalf("error executing first query: %v", err)
		return
	}
	if firstResults.Next() {
		firstTime := firstResults.Record().Time()
		startTsStr = firstTime.Format("2006-01-02T15:04:05.000Z")
	}
	if err := firstResults.Err(); err != nil {
		log.Fatalf("error reading first query results: %v", err)
	}

	// Execute the last query
	lastResults, err := queryAPI.Query(context.Background(), lastQuery)
	if err != nil {
		log.Fatalf("error executing last query: %v", err)
		return
	}
	if lastResults.Next() {
		lastTime := lastResults.Record().Time()
		endTsStr = lastTime.Format("2006-01-02T15:04:05.000Z")
	}
	if err := lastResults.Err(); err != nil {
		log.Fatalf("error reading last query results: %v", err)
	}

	return startTsStr, endTsStr
}

func ReadDataAndSendDirectly(client *influxdb2.Client, bucket, start, end, eqpId, topic string, producer *kafka.Producer) (int, float64) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)
	query := `
    from(bucket: "` + bucket + `")
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
