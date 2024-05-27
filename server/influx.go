package main

import (
	"context"
	"sort"

	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
)

func CheckStartEndRange(client *influxdb2.Client, bucket, measurement, tagKey, tagValue string) (startTsStr, endTsStr string) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)

	baseQuery := fmt.Sprintf(`
	from(bucket: "%s")
		|> range(start: 0)
		|> filter(fn: (r) => r._measurement == "%s")`, bucket, measurement)

	// Define the query for getting the first timestamp
	firstQuery := baseQuery
	if tagKey != "" && tagValue != "" {
		firstQuery += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`, tagKey, tagValue)
	} else {
		firstQuery += `|> group()`
	}
	firstQuery += `|> first()`

	// Define the query for getting the last timestamp
	lastQuery := baseQuery
	if tagKey != "" && tagValue != "" {
		lastQuery += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`, tagKey, tagValue)
	} else {
		lastQuery += `|> group()`
	}
	lastQuery += `|> last()`

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

func ReadDataAndSendDirectly(client *influxdb2.Client,
	start, end, bucket, measurement, tagKey, tagValue, topic string,
	producer *kafka.Producer) (int, float64) {

	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)
	query := fmt.Sprintf(`
	from(bucket: "%s")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "%s")
	`, bucket, start, end, measurement)

	if tagKey != "" && tagValue != "" {
		query += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`, tagKey, tagValue)
	}

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

func getMeasurements(client *influxdb2.Client) ([]Dataset, error) {
	org := "influxdata"

	// Create the Bucket API client
	bucketAPI := (*client).BucketsAPI()

	// Retrieve the list of buckets
	buckets, err := bucketAPI.FindBucketsByOrgName(context.Background(), org)
	if err != nil {
		return nil, err
	}

	// Retrieve the list of measurements for each bucket
	queryAPI := (*client).QueryAPI(org)
	var dataset_list []Dataset

	for _, bucket := range *buckets {
		query := fmt.Sprintf(`import "influxdata/influxdb/v1" v1.measurements(bucket: "%s")`, bucket.Name)
		result, err := queryAPI.Query(context.Background(), query)
		if err != nil {
			return nil, err
		}
		for result.Next() {
			measurement := result.Record().ValueByKey("_value").(string)

			tagKey_query := fmt.Sprintf(`
			import "influxdata/influxdb/schema"
			import "strings"
			
			schema.tagKeys(
			bucket: "%s",
			predicate: (r) => r._measurement == "%s",
			start: 0  // 데이터 조회 시간 범위 설정
			)
			|> filter(fn: (r) =>
				not strings.hasPrefix(v: r._value, prefix: "_")
			)
			`, bucket.Name, measurement)
			result2, err2 := queryAPI.Query(context.Background(), tagKey_query)
			if err2 != nil {
				return nil, err2
			}
			for result2.Next() {
				tagKey := result2.Record().ValueByKey("_value").(string)
				tagValue_query := fmt.Sprintf(`import "influxdata/influxdb/schema"

				schema.tagValues(
				bucket: "%s",
				predicate: (r) => r._measurement == "%s",
				tag: "%s",
				start: 0, 
				)`, bucket.Name, measurement, tagKey)
				result3, err3 := queryAPI.Query(context.Background(), tagValue_query)
				if err3 != nil {
					return nil, err3
				}
				for result3.Next() {
					tagValue := result3.Record().ValueByKey("_value").(string)

					dataset_list = append(dataset_list, Dataset{
						BucketName:  bucket.Name,
						Measurement: measurement,
						TagKeyStr:   tagKey,
						TagValueStr: tagValue,
					})
				}
			}
		}
	}

	return dataset_list, nil
}

func queryInfluxDB(client *influxdb2.Client, bucket, measurement, tagKey, tagValue string) (map[string]interface{}, error) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)

	startQuery := buildQuery(bucket, measurement, tagKey, tagValue, "first")
	endQuery := buildQuery(bucket, measurement, tagKey, tagValue, "last")
	countQuery := buildQuery(bucket, measurement, tagKey, tagValue, "count")

	// Execute start query
	startResult, err := queryAPI.Query(context.Background(), startQuery)
	if err != nil {
		return nil, err
	}

	var startTime time.Time
	var startValue string
	var columns []string
	var data []map[string]interface{}
	if startResult.Next() {
		startTime = startResult.Record().Time()
		startValue = startResult.Record().ValueByKey("_value").(string)

		var jsonData map[string]interface{}
		if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(startValue), &jsonData); err != nil {
			return nil, err
		}

		for key := range jsonData {
			columns = append(columns, key)
		}
		sort.Strings(columns) // Ensure columns are sorted
		data = append(data, jsonData)
	}
	if startResult.Err() != nil {
		return nil, startResult.Err()
	}

	// Execute end query
	endResult, err := queryAPI.Query(context.Background(), endQuery)
	if err != nil {
		return nil, err
	}

	var endTime time.Time
	var endValue string
	if endResult.Next() {
		endTime = endResult.Record().Time()
		endValue = endResult.Record().ValueByKey("_value").(string)

		var jsonData map[string]interface{}
		if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(endValue), &jsonData); err != nil {
			return nil, err
		}
		data = append(data, jsonData)
	}
	if endResult.Err() != nil {
		return nil, endResult.Err()
	}

	// Execute count query
	countResult, err := queryAPI.Query(context.Background(), countQuery)
	if err != nil {
		return nil, err
	}

	var count int64
	if countResult.Next() {
		count = countResult.Record().Value().(int64)
	}
	if countResult.Err() != nil {
		return nil, countResult.Err()
	}

	// Extract only the values for the data field
	dataValues := make([][]interface{}, len(data))
	for i, record := range data {
		values := make([]interface{}, len(columns))
		for j, column := range columns {
			values[j] = record[column]
		}
		dataValues[i] = values
	}

	// Prepare the response data
	responseData := map[string]interface{}{
		"start":       startTime.Format(time.RFC3339Nano),
		"end":         endTime.Format(time.RFC3339Nano),
		"bucket":      bucket,
		"measurement": measurement,
		"count":       count,
		"columns":     columns,
		"data":        dataValues,
	}

	return responseData, nil
}

func buildQuery(bucket, measurement, tagKey, tagValue, aggregation string) string {
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: 0)
			|> filter(fn: (r) => r._measurement == "%s")`, bucket, measurement)

	if tagKey != "" && tagValue != "" {
		query += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`, tagKey, tagValue)
	} else {
		query += `|> group()`
	}

	query += fmt.Sprintf(`
			|> %s()
			|> limit(n: 1)`, aggregation)

	return query
}
