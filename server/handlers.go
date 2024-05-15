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
	EqpIdStr      string `json:"eqpIdStr"`
	BucketStr     string `json:"bucketStr"`
	SendTopicStr  string `json:"sendTopicStr"`
	TotalMessages int    `json:"totalMessages"`
	StartTime     int64  `json:"startTimeMillis"`
	EndTime       int64  `json:"endTimeMillis"`
}

func handleRequests() {
	// 라우터 설정
	http.HandleFunc("/data-by-time-range/", handleRequestA)
	http.HandleFunc("/all-of-data-by-eqpid/", handleRequestB)
}

func handleRequestB(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		// 데이터 읽기
		eqpId := r.URL.Query().Get("eqp_id")
		bucket := r.URL.Query().Get("bucket")
		sendTopic := r.URL.Query().Get("send_topic")

		if !validateParams(w, []string{eqpId, bucket, sendTopic}) {
			return // Parameters missing, error response sent
		}

		fmt.Println(
			"Equipment ID:", eqpId,
			"bucket:", bucket,
			"sendTopic", sendTopic,
		)

		client := influxdb2.NewClientWithOptions(url, token,
			influxdb2.DefaultOptions().
				SetPrecision(time.Millisecond).
				SetHTTPRequestTimeout(900))
		defer client.Close()

		startTsStr, endTsStr := CheckStartEndRange(&client, bucket, eqpId)

		total_msg_count, start_time_millis, end_time_millis, err :=
			processJobs(startTsStr, endTsStr, bucket, eqpId, sendTopic)
		if err != nil {
			fmt.Println("Error parsing time:", err)
			return
		}

		// 응답 데이터 구성 및 JSON 직렬화
		// 응답 데이터 구성
		responseData := ResponseData{
			QStartStr:     startTsStr,
			QEndStr:       endTsStr,
			EqpIdStr:      eqpId,
			BucketStr:     bucket,
			SendTopicStr:  sendTopic,
			TotalMessages: total_msg_count,
			StartTime:     start_time_millis, // 밀리초 단위로 변환
			EndTime:       end_time_millis,   // 밀리초 단위로 변환
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
		eqpId := r.URL.Query().Get("eqp_id")
		bucket := r.URL.Query().Get("bucket")
		sendTopic := r.URL.Query().Get("send_topic")

		if !validateParams(w, []string{startStr, endStr, eqpId, bucket, sendTopic}) {
			return
		}

		fmt.Println(
			"startStr:", startStr,
			"endStr:", endStr,
			"Equipment ID:", eqpId,
			"bucket:", bucket,
			"sendTopic", sendTopic,
		)

		total_msg_count, start_time_millis, end_time_millis, err :=
			processJobs(startStr, endStr, bucket, eqpId, sendTopic)
		if err != nil {
			fmt.Println("Error on processJobs():", err)
			return
		}

		// 응답 데이터 구성 및 JSON 직렬화
		// 응답 데이터 구성
		responseData := ResponseData{
			QStartStr:     startStr,
			QEndStr:       endStr,
			EqpIdStr:      eqpId,
			BucketStr:     bucket,
			SendTopicStr:  sendTopic,
			TotalMessages: total_msg_count,
			StartTime:     start_time_millis, // 밀리초 단위로 변환
			EndTime:       end_time_millis,   // 밀리초 단위로 변환
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
func processJobs(startTsStr, endTsStr, bucket, eqpId, sendTopic string) (int, int64, int64, error) {

	joblist := []*Job{}
	messagesCh := make(chan int, config.Jobs.DividedJobs)
	wg := new(sync.WaitGroup)

	timePairs, err := divideTime(startTsStr, endTsStr)
	if err != nil {
		fmt.Println("Error parsing time:", err)
		return 0, 0, 0, err
	}

	for _, pair := range timePairs {
		joblist = append(joblist, newJob(bucket, pair.Start, pair.End, eqpId, sendTopic, &messagesCh, &wg))
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
