package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
)

type ErrorResponse struct {
	Message string `json:"message"`
}

type ResponseData struct {
	QStartStr     string `json:"queryStartStr"`
	QEndStr       string `json:"queryEndStr"`
	BucketStr     string `json:"bucketStr"`
	MeasurementStr 	string `json:"measurementStr"`
	TagKeyStr string `json:"tagKeyStr"`
	TagValueStr string `json:"tagValueStr"`
	SendTopicStr  string `json:"sendTopicStr"`
	TotalMessages int    `json:"totalMessages"`
	StartTime     int64  `json:"startTimeMillis"`
	EndTime       int64  `json:"endTimeMillis"`
}

type Measurement struct {
	BucketName  string `json:"bucket_name"`
	Measurement string `json:"measurement"`
}

func handleRequests() {
	// 라우터 설정
	http.HandleFunc("/data-by-time-range/", handleRequestA)
	http.HandleFunc("/all-of-data-by-eqpid/", handleRequestB)
	http.HandleFunc("/bucket-list/", handleRequestC)
	http.HandleFunc("/bucket-detail/", handleRequestD)
}

func handleRequestD(w http.ResponseWriter, r *http.Request) {
	// bucket-detail
	switch r.Method {
	case "GET":
		// 응답 데이터 구성 및 JSON 직렬화
		// 응답 데이터 구성
		// HTTP 응답 헤더 설정 및 JSON 응답 전송
		Bucket := r.URL.Query().Get("bucket_name")
		Measurement := r.URL.Query().Get("measurement")

		fmt.Println(
			"Bucket:", Bucket,
			"Measurement:", Measurement,
		)

		client := influxdb2.NewClientWithOptions(url, token,
			influxdb2.DefaultOptions().
				SetPrecision(time.Millisecond).
				SetHTTPRequestTimeout(900))
		defer client.Close()

		org := "influxdata"
		queryAPI := client.QueryAPI(org)

		// Flux queries
		startQuery := fmt.Sprintf(`
			from(bucket: "%s")
				|> range(start: 0)
				|> filter(fn: (r) => r._measurement == "%s")
				|> group()
				|> first()
				|> limit(n: 1)
		`, Bucket, Measurement)

		endQuery := fmt.Sprintf(`
			from(bucket: "%s")
				|> range(start: 0)
				|> filter(fn: (r) => r._measurement == "%s")
				|> group()
				|> last()
				|> limit(n: 1)
		`, Bucket, Measurement)

		countQuery := fmt.Sprintf(`
			from(bucket: "%s")
				|> range(start: 0)
				|> filter(fn: (r) => r._measurement == "%s")
				|> group()
				|> count()
		`, Bucket, Measurement)

		// Execute start query
		startResult, err := queryAPI.Query(context.Background(), startQuery)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			for key := range jsonData {
				columns = append(columns, key)
			}
			sort.Strings(columns) // Ensure columns are sorted
			data = append(data, jsonData)
		}
		if startResult.Err() != nil {
			http.Error(w, startResult.Err().Error(), http.StatusInternalServerError)
			return
		}

		// Execute end query
		endResult, err := queryAPI.Query(context.Background(), endQuery)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var endTime time.Time
		var endValue string
		if endResult.Next() {
			endTime = endResult.Record().Time()
			endValue = endResult.Record().ValueByKey("_value").(string)

			var jsonData map[string]interface{}
			if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(endValue), &jsonData); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			data = append(data, jsonData)
		}
		if endResult.Err() != nil {
			http.Error(w, endResult.Err().Error(), http.StatusInternalServerError)
			return
		}

		// Execute count query
		countResult, err := queryAPI.Query(context.Background(), countQuery)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var count int64
		if countResult.Next() {
			count = countResult.Record().Value().(int64)
		}
		if countResult.Err() != nil {
			http.Error(w, countResult.Err().Error(), http.StatusInternalServerError)
			return
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
			"bucket":      Bucket,
			"measurement": Measurement,
			"start":       startTime.Format(time.RFC3339Nano),
			"end":         endTime.Format(time.RFC3339Nano),
			"count":       count,
			"columns":     columns,
			"data":        dataValues,
		}

		// JSON serialization
		responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(responseJSON)
		return
	case "POST":
		fmt.Fprintf(w, "post request")
	default:
		fmt.Fprintf(w, "unknown request")
	}
}

func handleRequestC(w http.ResponseWriter, r *http.Request) {
	// /bucket-list/
	switch r.Method {
	case "GET":

		client := influxdb2.NewClientWithOptions(url, token,
			influxdb2.DefaultOptions().
				SetPrecision(time.Millisecond).
				SetHTTPRequestTimeout(900))
		defer client.Close()

		org := "influxdata"

		// 버킷 API 클라이언트 생성
		bucketAPI := client.BucketsAPI()

		// 버킷 목록 조회
		buckets, err := bucketAPI.FindBucketsByOrgName(context.Background(), org)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 각 버킷의 measurement 목록 조회
		queryAPI := client.QueryAPI(org)
		var measurements []Measurement

		for _, bucket := range *buckets {
			query := fmt.Sprintf(`import "influxdata/influxdb/v1" v1.measurements(bucket: "%s")`, bucket.Name)
			result, err := queryAPI.Query(context.Background(), query)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			for result.Next() {
				measurements = append(measurements, Measurement{
					BucketName:  bucket.Name,
					Measurement: result.Record().ValueByKey("_value").(string),
				})
			}
			if result.Err() != nil {
				http.Error(w, result.Err().Error(), http.StatusInternalServerError)
				return
			}
		}

		response := map[string]interface{}{
			"dataset_list": measurements,
		}

		// JSON 직렬화
		responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// HTTP 응답 헤더 설정 및 JSON 응답 전송
		w.Header().Set("Content-Type", "application/json")
		w.Write(responseJSON)
		return
	case "POST":
		fmt.Fprintf(w, "post request")
	default:
		fmt.Fprintf(w, "unknown request")
	}
}

func handleRequestB(w http.ResponseWriter, r *http.Request) {
	// /all-of-data-by-eqpid/
	switch r.Method {
	case "GET":
		// 데이터 읽기
		bucket := r.URL.Query().Get("bucket")
		measurement := r.URL.Query().Get("measurement")
		tagKey := r.URL.Query().Get("tag_key")
		tagValue := r.URL.Query().Get("tag_value")
		sendTopic := r.URL.Query().Get("send_topic")

		if !validateParams(w, []string{bucket, measurement, tagKey, tagValue, sendTopic}) {
			return // Parameters missing, error response sent
		}

		fmt.Println(
			"bucket:", bucket,
			"measurement:", measurement,
			"tagKey:", tagKey,
			"tagValue:", tagValue,
			"sendTopic", sendTopic,
		)

		client := influxdb2.NewClientWithOptions(url, token,
			influxdb2.DefaultOptions().
				SetPrecision(time.Millisecond).
				SetHTTPRequestTimeout(900))
		defer client.Close()

		startTsStr, endTsStr := CheckStartEndRange(&client, bucket, measurement, tagKey, tagValue)

		total_msg_count, start_time_millis, end_time_millis, err :=
			processJobs(startTsStr, endTsStr, bucket, measurement, tagKey, tagValue, sendTopic)
		if err != nil {
			fmt.Println("Error parsing time:", err)
			return
		}

		// 응답 데이터 구성 및 JSON 직렬화
		// 응답 데이터 구성
		responseData := ResponseData{
			QStartStr:     		startTsStr,
			QEndStr:       		endTsStr,
			BucketStr:     		bucket,
			MeasurementStr:     measurement,
			TagKeyStr:			tagKey,
			TagValueStr: 		tagValue,
			SendTopicStr:  		sendTopic,
			TotalMessages: 		total_msg_count,
			StartTime:     		start_time_millis, // 밀리초 단위로 변환
			EndTime:       		end_time_millis,   // 밀리초 단위로 변환
		}

		responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// HTTP 응답 헤더 설정 및 JSON 응답 전송
		w.Header().Set("Content-Type", "application/json")
		w.Write(responseJSON)
		return
	case "POST":
		fmt.Fprintf(w, "post request")
	default:
		fmt.Fprintf(w, "unknown request")
	}
}

func handleRequestA(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		// 데이터 읽기
		startStr := r.URL.Query().Get("start")
		endStr := r.URL.Query().Get("end")
		bucket := r.URL.Query().Get("bucket")
		measurement := r.URL.Query().Get("measurement")
		tagKey := r.URL.Query().Get("tag_key")
		tagValue := r.URL.Query().Get("tag_value")
		sendTopic := r.URL.Query().Get("send_topic")

		if !validateParams(w, []string{startStr, endStr, bucket, measurement, tagKey, tagValue, sendTopic}) {
			return
		}

		fmt.Println(
			"startStr:", startStr,
			"endStr:", endStr,
			"bucket:", bucket,
			"measurement:", measurement,
			"tagKey:", tagKey,
			"tagValue:", tagValue,
			"sendTopic", sendTopic,
		)

		total_msg_count, start_time_millis, end_time_millis, err :=
			processJobs(startStr, endStr, bucket, measurement, tagKey, tagValue, sendTopic)
		if err != nil {
			fmt.Println("Error on processJobs():", err)
			return
		}

		// 응답 데이터 구성 및 JSON 직렬화
		// 응답 데이터 구성
		responseData := ResponseData{
			QStartStr:     		startStr,
			QEndStr:       		endStr,
			BucketStr:     		bucket,
			MeasurementStr:     measurement,
			TagKeyStr:			tagKey,
			TagValueStr: 		tagValue,
			SendTopicStr:  		sendTopic,
			TotalMessages: 		total_msg_count,
			StartTime:     		start_time_millis, // 밀리초 단위로 변환
			EndTime:       		end_time_millis,   // 밀리초 단위로 변환
		}

		responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// HTTP 응답 헤더 설정 및 JSON 응답 전송
		w.Header().Set("Content-Type", "application/json")
		w.Write(responseJSON)
		return

	case "POST":
		fmt.Fprintf(w, "post request")
	default:
		fmt.Fprintf(w, "unknown request")
	}
}

// validateParams checks for missing parameters and sends an error response if any are missing
func validateParams(w http.ResponseWriter, values []string) bool {
	missingParams := []string{}

	// Check if any parameter is an empty string
	for _, value := range values {
		if value == "" {
			missingParams = append(missingParams, value)
		}
	}

	// If any parameters are missing, send an error response
	if len(missingParams) > 0 {
		errorResponse := ErrorResponse{Message: "Missing required parameters"}
		w.WriteHeader(http.StatusBadRequest) // 400 Bad Request
		json.NewEncoder(w).Encode(errorResponse)
		return false
	}
	return true
}

// processJobs processes the tasks within the provided time range and returns the results
func processJobs(startTsStr, endTsStr, bucket, measurement, tagKey, tagValue, sendTopic string) (int, int64, int64, error) {

	joblist := []*Job{}
	messagesCh := make(chan int, config.Jobs.DividedJobs)
	wg := new(sync.WaitGroup)

	timePairs, err := divideTime(startTsStr, endTsStr)
	if err != nil {
		fmt.Println("Error parsing time:", err)
		return 0, 0, 0, err
	}

	for _, pair := range timePairs {
		joblist = append(joblist, newJob(pair.Start, pair.End, bucket, measurement, tagKey, tagValue, sendTopic, &messagesCh, &wg))
	}

	startTime := time.Now()

	// Create and queue jobs
	for _, job := range joblist {
		wg.Add(1)
		JobQueue <- *job
	}

	// Wait for all jobs to complete
	wg.Wait()

	endTime := time.Now()
	elapsed := time.Since(startTime)

	// Aggregate message count
	totalMsgCount := 0
	for i := 0; i < len(joblist); i++ {
		totalMsgCount += <-messagesCh
	}

	fmt.Printf("Total processed messages: %d, Total took: %s\n", totalMsgCount, elapsed)
	return totalMsgCount, startTime.UnixNano() / 1e6, endTime.UnixNano() / 1e6, nil
}
