package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/noFlowWater/large-dataset-delivery-kafka/server/examplepb"
	"google.golang.org/protobuf/proto"
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
		for result.Next() {
			measurement := result.Record().ValueByKey("_value").(string)

			tagKey_query := fmt.Sprintf(`
			import "influxdata/influxdb/schema"
			import "strings"
			
			schema.tagKeys(
			bucket: "%s",
			predicate: (r) => r._measurement == "%s",
			start: 0
			)
			|> filter(fn: (r) =>
				not strings.hasPrefix(v: r._value, prefix: "_")
			)
			|> filter(fn: (r) =>
				not strings.hasPrefix(v: r._value, prefix: "tag_key")
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

// QueryDatabase implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) QueryDatabase(params QueryParams) (map[string]interface{}, error) {
	queryAPI := (*i.client).QueryAPI(i.org)
	influxParams := params.GetInfluxParams()

	startQuery := i.buildQuery(influxParams.Bucket, influxParams.Measurement, influxParams.TagKey, influxParams.TagValue, "first")
	endQuery := i.buildQuery(influxParams.Bucket, influxParams.Measurement, influxParams.TagKey, influxParams.TagValue, "last")
	countQuery := i.buildQuery(influxParams.Bucket, influxParams.Measurement, influxParams.TagKey, influxParams.TagValue, "count")

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

		// Decode startValue
		dataElement, err := decodeValue(influxParams.Bucket, influxParams.Measurement, startValue)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		// Extract columns from dataElement and sort them
		for key := range dataElement {
			columns = append(columns, key)
		}
		sort.Strings(columns)

		// Append dataElement to data slice
		data = append(data, dataElement)
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

		// Decode endValue
		dataElement, err := decodeValue(influxParams.Bucket, influxParams.Measurement, endValue)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		// Append dataElement to data slice
		data = append(data, dataElement)
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
		"bucket":      influxParams.Bucket,
		"measurement": influxParams.Measurement,
		"count":       count,
		"columns":     columns,
		"data":        dataValues,
	}

	return responseData, nil
}

// ReadDataAndSend implements DatabaseClient interface for InfluxDB
func (i *InfluxDBClient) ReadDataAndSend(params QueryParams, execInfo JobExecutionInfo, producer *kafka.Producer, topic string, partitionCount int) (int, error) {
	queryAPI := (*i.client).QueryAPI(i.org)
	influxParams := params.GetInfluxParams()

	query := fmt.Sprintf(`
	from(bucket: "%s")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "%s")
	`, influxParams.Bucket, execInfo.StartStr, execInfo.EndStr, influxParams.Measurement)

	if influxParams.TagKey != "" && influxParams.TagValue != "" {
		query += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`,
			influxParams.TagKey, influxParams.TagValue)
	}

	results, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return 0, err
	}

	totalProcessed := 0
	for results.Next() {
		record := results.Record()
		if v, ok := record.Values()["_value"].(string); ok {
			// 데이터 처리
			dataMap := map[string]interface{}{
				"value": decodeBase64(v),
				"time":  record.Time(),
			}

			// JSON으로 변환
			jsonData, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(dataMap)
			if err != nil {
				log.Printf("Error marshaling record: %v", err)
				continue
			}

			// 파티션 결정
			var partition int32 = kafka.PartitionAny
			if execInfo.SendPartition >= 0 {
				partition = int32(execInfo.SendPartition)
			} else {
				partition = int32(totalProcessed % partitionCount)
			}

			// Kafka로 메시지 전송
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: partition,
				},
				Value: jsonData,
			}, nil)
			totalProcessed++
		}
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
func decodeBase64(encoded string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		fmt.Println(err)
	}
	return decoded
}

func decodeValue(bucket, measurement, startValue string) (map[string]interface{}, error) {
	key := fmt.Sprintf("%s:%s", bucket, measurement)
	if decodeFunc, exists := decoderMap[key]; exists {
		return decodeFunc(startValue)
	}

	var jsonData map[string]interface{}
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(startValue), &jsonData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	return jsonData, nil
}

// Add decoder functions and maps
type decodeFunc func(string) (map[string]interface{}, error)

func unmarshalProtobuf[T proto.Message](data []byte, message T) error {
	if err := proto.Unmarshal(data, message); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %v", err)
	}
	return nil
}

func convertToMap[T proto.Message](message T, converter func(T) map[string]interface{}) map[string]interface{} {
	return converter(message)
}

func decodeBase64_unmarshalProtobuf_convertToMap[T proto.Message](
	startValue string, message T, converter func(T) map[string]interface{}) (
	map[string]interface{}, error) {

	// Base64 디코드
	decoded := decodeBase64(startValue)

	// 프로토버프 언마샬
	if err := unmarshalProtobuf(decoded, message); err != nil {
		return nil, err
	}

	// 메시지를 맵으로 변환
	resultMap := convertToMap(message, converter)
	return resultMap, nil
}

var decoderMap = map[string]decodeFunc{
	"mqtt_iot_sensor:transport": func(startValue string) (map[string]interface{}, error) {
		var transport examplepb.Transport
		return decodeBase64_unmarshalProtobuf_convertToMap(startValue, &transport, transportToMap)
	},
	"electric:electric_dataset": func(startValue string) (map[string]interface{}, error) {
		var electric examplepb.Electric
		return decodeBase64_unmarshalProtobuf_convertToMap(startValue, &electric, electricToMap)
	},
}

func electricToMap(electric *examplepb.Electric) map[string]interface{} {
	return map[string]interface{}{
		"building_number":   electric.BuildingNumber,
		"temperature":       electric.Temperature,
		"rainfall":          electric.Rainfall,
		"windspeed":         electric.Windspeed,
		"humidity":          electric.Humidity,
		"power_consumption": electric.PowerConsumption,
		"month":             electric.Month,
		"day":               electric.Day,
		"time":              electric.Time,
		"total_area":        electric.TotalArea,
		"building_type":     electric.BuildingType,
	}
}

func transportToMap(transport *examplepb.Transport) map[string]interface{} {
	return map[string]interface{}{
		"index":          transport.Index,
		"blk_no":         transport.BlkNo,
		"press3":         transport.Press3,
		"calc_press2":    transport.CalcPress2,
		"press4":         transport.Press4,
		"calc_press1":    transport.CalcPress1,
		"calc_press4":    transport.CalcPress4,
		"calc_press3":    transport.CalcPress3,
		"bf_gps_lon":     transport.BfGpsLon,
		"gps_lat":        transport.GpsLat,
		"speed":          transport.Speed,
		"in_dt":          transport.InDt,
		"move_time":      transport.MoveTime,
		"dvc_id":         transport.DvcId,
		"dsme_lat":       transport.DsmeLat,
		"press1":         transport.Press1,
		"press2":         transport.Press2,
		"work_status":    transport.WorkStatus,
		"timestamp":      transport.Timestamp,
		"is_adjust":      transport.IsAdjust,
		"move_distance":  transport.MoveDistance,
		"weight":         transport.Weight,
		"dsme_lon":       transport.DsmeLon,
		"in_user":        transport.InUser,
		"eqp_id":         transport.EqpId,
		"blk_get_seq_id": transport.BlkGetSeqId,
		"lot_no":         transport.LotNo,
		"proj_no":        transport.ProjNo,
		"gps_lon":        transport.GpsLon,
		"seq_id":         transport.SeqId,
		"bf_gps_lat":     transport.BfGpsLat,
		"blk_dvc_id":     transport.BlkDvcId,
	}
}

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
