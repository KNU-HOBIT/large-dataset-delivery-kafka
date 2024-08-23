package main

import (
	"context"
	"encoding/base64"
	"sort"

	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/noFlowWater/large-dataset-delivery-kafka/server/examplepb"
	"google.golang.org/protobuf/proto"
)

func CheckStartEndRange(client *influxdb2.Client, params InfluxQueryParams) (TimeRangeStr, error) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)

	baseQuery := fmt.Sprintf(`
	from(bucket: "%s")
		|> range(start: 0)
		|> filter(fn: (r) => r._measurement == "%s")`,
		params.bucket, params.measurement)

	// Define the query for getting the first timestamp
	firstQuery := baseQuery
	if params.tagKey != "" && params.tagValue != "" {
		firstQuery += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`,
			params.tagKey, params.tagValue)
	} else {
		firstQuery += `|> group()`
	}
	firstQuery += `|> first()`

	// Define the query for getting the last timestamp
	lastQuery := baseQuery
	if params.tagKey != "" && params.tagValue != "" {
		lastQuery += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`,
			params.tagKey, params.tagValue)
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
		startTsStr = firstTime.Format("2006-01-02T15:04:05.000Z")
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
		endTsStr = lastTime.Format("2006-01-02T15:04:05.000Z")
	}
	if err := lastResults.Err(); err != nil {
		return TimeRangeStr{}, fmt.Errorf("error reading last query results: %v", err)
	}

	return TimeRangeStr{startStr: startTsStr, endStr: endTsStr}, nil
}

func getTotalRecordCount(client *influxdb2.Client, ts TimeRangeStr, q_params InfluxQueryParams) (int64, error) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)
	query := fmt.Sprintf(`
        from(bucket:"%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "%s")`,
		q_params.bucket, ts.startStr, ts.endStr, q_params.measurement)

	if q_params.tagKey != "" && q_params.tagValue != "" {
		query += fmt.Sprintf(`|> filter(fn: (r) => r["%s"] == "%s")`,
			q_params.tagKey, q_params.tagValue)
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

func calculateEndTimes(client *influxdb2.Client, startStr string, recordsPerJob int64, q_params InfluxQueryParams) ([]string, error) {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)

	query := fmt.Sprintf(`
        from(bucket:"%s")
        |> range(start: %s)
        |> filter(fn: (r) => r._measurement == "%s")`,
		q_params.bucket, startStr, q_params.measurement)

	if q_params.tagKey != "" && q_params.tagValue != "" {
		query += fmt.Sprintf(`
        |> filter(fn: (r) => r["%s"] == "%s")`,
			q_params.tagKey, q_params.tagValue)
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

type decodeFunc func(string) (map[string]interface{}, error)

func decodeBase64(encoded string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		fmt.Println(err)
	}
	return decoded
}

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

// transportToMap converts Protobuf Transport message to map
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

		// Decode startValue
		dataElement, err := decodeValue(bucket, measurement, startValue)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		// Extract columns from dataElement and sort them
		for key := range dataElement {
			columns = append(columns, key)
		}
		sort.Strings(columns) // Ensure columns are sorted

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

		// Decode startValue
		dataElement, err := decodeValue(bucket, measurement, endValue)
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

// func fetchSchema(client *influxdb2.Client, bucket, measurement, tagKey string) ([]SchemaField, error) {
// 	org := "influxdata"
// 	queryAPI := (*client).QueryAPI(org)

// 	schema_query := fmt.Sprintf(`from(bucket: "%s")
// 	|> range(start: 0)
// 	|> filter(fn: (r) => r["_measurement"] == "%s")
// 	|> filter(fn: (r) => r["tag_key"] == "%s")
// 	|> last()`, bucket, measurement, tagKey)

// 	schema_result, err := queryAPI.Query(context.Background(), schema_query)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var schemaObj Schema
// 	if schema_result.Next() {
// 		schemaData := schema_result.Record().ValueByKey("_value").(string)
// 		err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(schemaData), &schemaObj)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	return schemaObj.Schema, nil
// }
