package main

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	http.HandleFunc("/all-of-data/", handleRequestB)
	http.HandleFunc("/bucket-list/", handleRequestC)
	http.HandleFunc("/bucket-detail/", handleRequestD)
}

func handleRequestD(w http.ResponseWriter, r *http.Request) {
	// bucket-detail
	switch r.Method {
	case "GET":
		
		Bucket := r.URL.Query().Get("bucket_name")
		Measurement := r.URL.Query().Get("measurement")
		tagKey := r.URL.Query().Get("tag_key")
		tagValue := r.URL.Query().Get("tag_value")

		if !validateParams(w, []string{Bucket, Measurement}) {
			return // Parameters missing, error response sent
		}

		fmt.Println(
			"tagKey:", tagKey,
			"tagValue:", tagValue,
			"Bucket:", Bucket,
			"Measurement:", Measurement,
		)

		client := influxdb2.NewClientWithOptions(url, token,
			influxdb2.DefaultOptions().
				SetPrecision(time.Millisecond).
				SetHTTPRequestTimeout(900))
		defer client.Close()
		
		responseData, err := queryInfluxDB(&client, Bucket, Measurement, tagKey, tagValue)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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

		measurements, err := getMeasurements(&client)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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

		if !validateParams(w, []string{bucket, measurement, sendTopic}) {
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

		go func() {
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
		}()
		return
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
		tagKey := r.URL.Query().Get("tag_key") // null 일 경우 모든 tag 데이터에 대해서 요청.
		tagValue := r.URL.Query().Get("tag_value") // null 일 경우 모든 tag 데이터에 대해서 요청.
		sendTopic := r.URL.Query().Get("send_topic")

		if !validateParams(w, []string{startStr, endStr, bucket, measurement, sendTopic}) {return}
		// tag_key, tag_value 검사X

		fmt.Println(
			"startStr:", startStr,
			"endStr:", endStr,
			"bucket:", bucket,
			"measurement:", measurement,
			"tagKey:", tagKey,
			"tagValue:", tagValue,
			"sendTopic", sendTopic,
		)

		go func() {
			total_msg_count, start_time_millis, end_time_millis, err := 
				processJobs(startStr, endStr, bucket, measurement, tagKey, tagValue, sendTopic)
			if err != nil {
				fmt.Println("Error on processJobs():", err)
				return
			}

			// 응답 데이터 구성 및 JSON 직렬화
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
		}()
		return
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
