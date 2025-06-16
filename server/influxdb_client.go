package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
)

// InfluxDBClient implements DatabaseClient for InfluxDB
type InfluxDBClient struct {
	client *influxdb2.Client
	url    string
	token  string
	org    string
}

// NewInfluxDBClient creates a new InfluxDB client
func NewInfluxDBClient(url, token, org string) (*InfluxDBClient, error) {
	client := influxdb2.NewClientWithOptions(url, token,
		influxdb2.DefaultOptions().
			SetPrecision(time.Millisecond).
			SetHTTPRequestTimeout(uint(config.Database.InfluxDB.HTTPRequestTimeoutSeconds)))

	return &InfluxDBClient{
		client: &client,
		url:    url,
		token:  token,
		org:    org,
	}, nil
}

// CheckStartEndRange implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) CheckStartEndRange(params QueryParams) (TimeRangeStr, error) {
	queryAPI := (*i.client).QueryAPI(i.org)

	influxParams := params.GetInfluxParams()

	baseQuery := fmt.Sprintf(`
	from(bucket: "%s")
		|> range(start: 0)
		|> filter(fn: (r) => r._measurement == "%s")`,
		influxParams.Bucket, influxParams.Measurement)

	// Define the query for getting the first timestamp
	firstQuery := baseQuery
	if influxParams.TagKey != "" && influxParams.TagValue != "" {
		firstQuery += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`,
			influxParams.TagKey, influxParams.TagValue)
	} else {
		firstQuery += `|> group()`
	}
	firstQuery += `|> first()`

	// Define the query for getting the last timestamp
	lastQuery := baseQuery
	if influxParams.TagKey != "" && influxParams.TagValue != "" {
		lastQuery += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`,
			influxParams.TagKey, influxParams.TagValue)
	} else {
		lastQuery += `|> group()`
	}
	lastQuery += `|> last()`

	var startTsStr, endTsStr string

	// Execute the first query
	firstResults, err := queryAPI.Query(context.Background(), firstQuery)
	if err != nil {
		return TimeRangeStr{}, fmt.Errorf("error executing first query: %v", err)
	}
	if firstResults.Next() {
		firstTime := firstResults.Record().Time()
		startTsStr = firstTime.Format(config.Formats.DateTimeFormat)
	}
	if err := firstResults.Err(); err != nil {
		return TimeRangeStr{}, fmt.Errorf("error reading first query results: %v", err)
	}

	// Execute the last query
	lastResults, err := queryAPI.Query(context.Background(), lastQuery)
	if err != nil {
		return TimeRangeStr{}, fmt.Errorf("error executing last query: %v", err)
	}
	if lastResults.Next() {
		lastTime := lastResults.Record().Time()
		endTsStr = lastTime.Format(config.Formats.DateTimeFormat)
	}
	if err := lastResults.Err(); err != nil {
		return TimeRangeStr{}, fmt.Errorf("error reading last query results: %v", err)
	}

	return TimeRangeStr{startStr: startTsStr, endStr: endTsStr}, nil
}

// GetTotalRecordCount implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) GetTotalRecordCount(timeRange TimeRangeStr, params QueryParams) (int64, error) {
	queryAPI := (*i.client).QueryAPI(i.org)
	influxParams := params.GetInfluxParams()

	query := fmt.Sprintf(`
        from(bucket:"%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "%s")`,
		influxParams.Bucket, timeRange.startStr, timeRange.endStr, influxParams.Measurement)

	if influxParams.TagKey != "" && influxParams.TagValue != "" {
		query += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`,
			influxParams.TagKey, influxParams.TagValue)
	} else {
		query += `|> group()`
	}

	query += `|> count()`

	result, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return 0, err
	}

	var totalCount int64
	for result.Next() {
		totalCount = result.Record().Value().(int64)
	}

	return totalCount, nil
}

// CalculateEndTimes implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) CalculateEndTimes(startStr string, recordsPerJob int64, params QueryParams) ([]string, error) {
	queryAPI := (*i.client).QueryAPI(i.org)
	influxParams := params.GetInfluxParams()

	query := fmt.Sprintf(`
        from(bucket:"%s")
        |> range(start: %s)
        |> filter(fn: (r) => r._measurement == "%s")`,
		influxParams.Bucket, startStr, influxParams.Measurement)

	if influxParams.TagKey != "" && influxParams.TagValue != "" {
		query += fmt.Sprintf(`
        |> filter(fn: (r) => r["%s"] == "%s")`,
			influxParams.TagKey, influxParams.TagValue)
	} else {
		query += `|> group()`
	}

	query += fmt.Sprintf(`
        |> sort(columns: ["_time"], desc: false)
        |> map(fn: (r) => ({r with index: 1}))
        |> cumulativeSum(columns: ["index"])
        |> filter(fn: (r) => r.index %% %d == 0)
        |> keep(columns: ["_time"])`, recordsPerJob)

	result, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}

	var endTimes []string
	for result.Next() {
		endTimes = append(endTimes,
			increaseOneNanosecond(result.Record().Time()).Format(time.RFC3339Nano))
	}

	if err := result.Err(); err != nil {
		return nil, err
	}

	return endTimes, nil
}

// GetDatasets implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) GetDatasets() ([]Dataset, error) {
	// Create the Bucket API client
	bucketAPI := (*i.client).BucketsAPI()

	// Retrieve the list of buckets
	buckets, err := bucketAPI.FindBucketsByOrgName(context.Background(), i.org)
	if err != nil {
		return nil, err
	}

	// Retrieve the list of measurements for each bucket
	queryAPI := (*i.client).QueryAPI(i.org)
	var dataset_list []Dataset

	for _, bucket := range *buckets {
		query := fmt.Sprintf(`import "influxdata/influxdb/v1" v1.measurements(bucket: "%s")`, bucket.Name)
		result, err := queryAPI.Query(context.Background(), query)
		if err != nil {
			return nil, err
		}

		var measurements []string
		for result.Next() {
			measurements = append(measurements, result.Record().Value().(string))
		}

		if err := result.Err(); err != nil {
			return nil, err
		}

		// Sort measurements alphabetically
		sort.Strings(measurements)

		// Create a dataset for each measurement
		for _, measurement := range measurements {
			dataset := Dataset{
				DatabaseName:       bucket.Name, // InfluxDB에서는 bucket이 database 역할
				TableName:          measurement, // measurement가 table 역할
				TagKeyStr:          "",
				TagValueStr:        "",
				DatasetName:        fmt.Sprintf("%s:%s", bucket.Name, measurement),
				DatasetDescription: fmt.Sprintf("Measurement '%s' in bucket '%s'", measurement, bucket.Name),
				DatasetType:        "InfluxDB",
				DatasetParams: map[string]interface{}{
					"bucket":      bucket.Name,
					"measurement": measurement,
				},
			}
			dataset_list = append(dataset_list, dataset)
		}
	}

	return dataset_list, nil
}

// QueryDatabase implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) QueryDatabase(params QueryParams) (map[string]interface{}, error) {
	queryAPI := (*i.client).QueryAPI(i.org)
	influxParams := params.GetInfluxParams()

	// Build the query
	query := i.buildQuery(influxParams.Bucket, influxParams.Measurement, influxParams.TagKey, influxParams.TagValue, "count")

	// Execute the query
	result, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}

	// Process the result
	var resultData map[string]interface{}
	for result.Next() {
		record := result.Record()
		resultData = map[string]interface{}{
			"time":  record.Time(),
			"value": record.Value(),
		}
		break // We only need the first (and only) result
	}

	if err := result.Err(); err != nil {
		return nil, err
	}

	return resultData, nil
}

// ReadDataAndSend implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) ReadDataAndSend(params QueryParams, execInfo JobExecutionInfo, producer *kafka.Producer, topic string, partitionCount int) (int, error) {
	queryAPI := (*i.client).QueryAPI(i.org)
	influxParams := params.GetInfluxParams()

	query := fmt.Sprintf(`
        from(bucket:"%s")
	|> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "%s")`,
		influxParams.Bucket, execInfo.StartStr, execInfo.EndStr, influxParams.Measurement)

	if influxParams.TagKey != "" && influxParams.TagValue != "" {
		query += fmt.Sprintf(`
        |> filter(fn: (r) => r["%s"] == "%s")`,
			influxParams.TagKey, influxParams.TagValue)
	}

	query += `
        |> sort(columns: ["_time"], desc: false)`

	results, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return 0, err
	}

	totalProcessed := 0
	for results.Next() {
		record := results.Record()

		// 레코드를 JSON으로 변환
		recordMap := make(map[string]interface{})
		recordMap["_time"] = record.Time()
		recordMap["_measurement"] = record.Measurement()

		// 모든 필드와 태그 추가
		for key, value := range record.Values() {
			if key != "_time" && key != "_measurement" {
				recordMap[key] = value
			}
		}

		jsonData, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(recordMap)
		if err != nil {
			log.Printf("Error marshaling record to JSON: %v", err)
			continue
		}

		// 파티션 결정
		partition := kafka.PartitionAny
		if execInfo.SendPartition >= 0 {
			partition = int32(execInfo.SendPartition)
		} else {
			partition = int32(totalProcessed % partitionCount)
		}

		// Kafka로 메시지 전송
		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
			},
			Value: jsonData,
		}, nil); err != nil {
			log.Printf("Error producing message to Kafka: %v", err)
			continue
		}
		totalProcessed++
	}

	if err := results.Err(); err != nil {
		return totalProcessed, err
	}

	return totalProcessed, nil
}

// buildQuery builds InfluxDB query
func (i *InfluxDBClient) buildQuery(bucket, measurement, tagKey, tagValue, aggregation string) string {
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

// Close closes the InfluxDB client
func (i *InfluxDBClient) Close() error {
	(*i.client).Close()
	return nil
}

// Missing functions that are referenced in other files
func (i *InfluxDBClient) CalculateJobExecutions(totalRecords int64, jobCount int, params QueryParams) ([]JobExecutionInfo, error) {
	// InfluxDB는 기존 시간 기반 분할 방식 사용
	recordsPerJob := totalRecords / int64(jobCount)

	// 기존 CalculateEndTimes 로직 활용
	timeRange, _ := i.CheckStartEndRange(params)
	endTimes, err := i.CalculateEndTimes(timeRange.startStr, recordsPerJob, params)
	if err != nil {
		return nil, err
	}

	var execInfos []JobExecutionInfo
	startStr := timeRange.startStr

	for _, endStr := range endTimes {
		execInfos = append(execInfos, JobExecutionInfo{
			StartStr: startStr,
			EndStr:   endStr,
			// Offset fields는 사용하지 않음 (기본값 0)
		})
		startStr = endStr
	}

	return execInfos, nil
}
