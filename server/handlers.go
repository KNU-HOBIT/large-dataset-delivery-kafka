package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
)

type ErrorResponse struct {
	Message string `json:"message"`
}

type TimeRangeStr struct {
	startStr string
	endStr   string
}

type KafkaEndPoint struct {
	bootstrapServers string
	topic            string
}

type InfluxQueryParams struct {
	url         string
	token       string
	org         string
	bucket      string
	measurement string
	tagKey      string
	tagValue    string
}

// JobParameters holds the parameters required for job processing
type JobParameters struct {
	Client      *influxdb2.Client
	StartTsStr  string
	EndTsStr    string
	Bucket      string
	Measurement string
	TagKey      string
	TagValue    string
	SendTopic   string
	DropLast    bool
}

type ResponseData struct {
	QStartStr       string   `json:"queryStartStr"`
	QEndStr         string   `json:"queryEndStr"`
	BucketStr       string   `json:"bucketStr"`
	MeasurementStr  string   `json:"measurementStr"`
	TagKeyStr       string   `json:"tagKeyStr"`
	TagValueStr     string   `json:"tagValueStr"`
	KafkaBrokersStr string   `json:"kafkaBrokersStr"`
	SendTopicStr    string   `json:"sendTopicStr"`
	TotalMessages   int      `json:"totalMessages"`
	StartTime       int64    `json:"startTimeMillis"`
	EndTime         int64    `json:"endTimeMillis"`
	OffsetsData     []string `json:"offsetsData"`
}

type Dataset struct {
	BucketName  string `json:"bucket_name"`
	Measurement string `json:"measurement"`
	TagKeyStr   string `json:"tagKeyStr"`
	TagValueStr string `json:"tagValueStr"`
}

func handleRequests() {
	// 라우터 설정
	http.HandleFunc("/data-by-time-range/", handleRequestA)
	http.HandleFunc("/all-of-data/", handleRequestB)
	http.HandleFunc("/bucket-list/", handleRequestC)
	http.HandleFunc("/bucket-detail/", handleRequestD)
	http.HandleFunc("/check-elapsed/", handleRequestE)
}

// /check-elapsed/
func handleRequestE(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		// 데이터 읽기
		bucket := r.URL.Query().Get("bucket")
		measurement := r.URL.Query().Get("measurement")
		tagKey := r.URL.Query().Get("tag_key")
		tagValue := r.URL.Query().Get("tag_value")

		if !validateParams(w, []string{bucket, measurement}) {
			return // Parameters missing, error response sent
		}

		fmt.Println(
			"bucket:", bucket,
			"measurement:", measurement,
			"tagKey:", tagKey,
			"tagValue:", tagValue,
		)

		q_params := InfluxQueryParams{
			bucket:      bucket,
			measurement: measurement,
			tagKey:      tagKey,
			tagValue:    tagValue,
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := influxdb2.NewClientWithOptions(url, token,
				influxdb2.DefaultOptions().
					SetPrecision(time.Millisecond).
					SetHTTPRequestTimeout(900))
			defer client.Close()

			timeRange, err := CheckStartEndRange(&client, q_params)
			if err != nil {
				log.Fatalf("Failed to get time range: %v", err)
			}

			fmt.Println("CheckStartEndRange : ", timeRange.startStr, timeRange.endStr)
			// 응답 데이터 구성 및 JSON 직렬화
			responseData := ResponseData{
				QStartStr:      timeRange.startStr,
				QEndStr:        timeRange.endStr,
				BucketStr:      bucket,
				MeasurementStr: measurement,
				TagKeyStr:      tagKey,
				TagValueStr:    tagValue,
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
		wg.Wait() // 모든 고루틴이 종료될 때까지 대기
		return
	default:
		fmt.Fprintf(w, "unknown request")
	}
}

// /bucket-detail/
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
		// tagKey: building_number tagValue: 1 Bucket: electric Measurement: electric_dataset
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

// /bucket-list/
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

// /all-of-data/
func handleRequestB(w http.ResponseWriter, r *http.Request) {
	// /all-of-data/
	switch r.Method {
	case "GET":
		// 데이터 읽기
		influxURL := r.URL.Query().Get("influx_url")
		influxToken := r.URL.Query().Get("influx_token")
		influxOrg := r.URL.Query().Get("influx_org")
		bucket := r.URL.Query().Get("bucket")
		measurement := r.URL.Query().Get("measurement")
		tagKey := r.URL.Query().Get("tag_key")
		tagValue := r.URL.Query().Get("tag_value")
		kafkaBrokers := r.URL.Query().Get("kafka_brokers")
		sendTopic := r.URL.Query().Get("send_topic")
		dropLastParam := r.URL.Query().Get("drop_last")

		if !validateParams(w, []string{influxURL, influxToken, influxOrg, bucket, measurement, kafkaBrokers, sendTopic}) {
			return // Parameters missing, error response sent
		}

		dropLast := true
		if dropLastParam == "false" || dropLastParam == "F" {
			dropLast = false
		}

		fmt.Println(
			"influx_url:", influxURL,
			"influx_org:", influxOrg,
			"bucket:", bucket,
			"measurement:", measurement,
			"tagKey:", tagKey,
			"tagValue:", tagValue,
			"kafkaBrokers:", kafkaBrokers,
			"sendTopic:", sendTopic,
		)

		k_endpoint := KafkaEndPoint{
			bootstrapServers: kafkaBrokers,
			topic:            sendTopic,
		}

		q_params := InfluxQueryParams{
			url:         influxURL,
			token:       influxToken,
			org:         influxOrg,
			bucket:      bucket,
			measurement: measurement,
			tagKey:      tagKey,
			tagValue:    tagValue,
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := influxdb2.NewClientWithOptions(influxURL, influxToken,
				influxdb2.DefaultOptions().
					SetPrecision(time.Millisecond).
					SetHTTPRequestTimeout(900))
			defer client.Close()

			timeRange, err := CheckStartEndRange(&client, q_params)
			if err != nil {
				log.Fatalf("Failed to get time range: %v", err)
			}

			total_msg_count, start_time_millis, end_time_millis, offsetsData, err :=
				processJobs(&client, timeRange, q_params, k_endpoint, dropLast)
			if err != nil {
				fmt.Println("Error parsing time:", err)
				return
			}

			// 응답 데이터 구성 및 JSON 직렬화
			responseData := ResponseData{
				QStartStr:      timeRange.startStr,
				QEndStr:        timeRange.endStr,
				BucketStr:      bucket,
				MeasurementStr: measurement,
				TagKeyStr:      tagKey,
				TagValueStr:    tagValue,
				SendTopicStr:   sendTopic,
				TotalMessages:  total_msg_count,
				StartTime:      start_time_millis, // 밀리초 단위로 변환
				EndTime:        end_time_millis,   // 밀리초 단위로 변환
				OffsetsData:    offsetsData,
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
		wg.Wait() // 모든 고루틴이 종료될 때까지 대기
		return
	default:
		fmt.Fprintf(w, "unknown request")
	}
}

// /data-by-time-range/
func handleRequestA(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		// 데이터 읽기
		influxURL := r.URL.Query().Get("influx_url")
		influxToken := r.URL.Query().Get("influx_token")
		influxOrg := r.URL.Query().Get("influx_org")
		startStr := r.URL.Query().Get("start")
		endStr := r.URL.Query().Get("end")
		bucket := r.URL.Query().Get("bucket")
		measurement := r.URL.Query().Get("measurement")
		tagKey := r.URL.Query().Get("tag_key")
		tagValue := r.URL.Query().Get("tag_value")
		kafkaBrokers := r.URL.Query().Get("kafka_brokers")
		sendTopic := r.URL.Query().Get("send_topic")
		dropLastParam := r.URL.Query().Get("drop_last")

		if !validateParams(w, []string{influxURL, influxToken, influxOrg, startStr, endStr, bucket, measurement, kafkaBrokers, sendTopic}) {
			return
		}
		// tag_key, tag_value 검사X

		fmt.Println(
			"influx_url:", influxURL,
			"influx_org:", influxOrg,
			"startStr:", startStr,
			"endStr:", endStr,
			"bucket:", bucket,
			"measurement:", measurement,
			"tagKey:", tagKey,
			"tagValue:", tagValue,
			"kafkaBrokers", kafkaBrokers,
			"sendTopic", sendTopic,
			"drop_last", dropLastParam,
		)

		dropLast := true
		if dropLastParam == "false" || dropLastParam == "F" {
			dropLast = false
		}

		k_endpoint := KafkaEndPoint{
			bootstrapServers: kafkaBrokers,
			topic:            sendTopic,
		}

		q_params := InfluxQueryParams{
			url:         influxURL,
			token:       influxToken,
			org:         influxOrg,
			bucket:      bucket,
			measurement: measurement,
			tagKey:      tagKey,
			tagValue:    tagValue,
		}

		t_range := TimeRangeStr{
			startStr: startStr,
			endStr:   endStr,
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := influxdb2.NewClientWithOptions(influxURL, influxToken,
				influxdb2.DefaultOptions().
					SetPrecision(time.Millisecond).
					SetHTTPRequestTimeout(900))
			defer client.Close()

			total_msg_count, start_time_millis, end_time_millis, offsetsData, err :=
				processJobs(&client, t_range, q_params, k_endpoint, dropLast)
			if err != nil {
				fmt.Println("Error on processJobs():", err)
				return
			}

			// 응답 데이터 구성 및 JSON 직렬화
			responseData := ResponseData{
				QStartStr:       startStr,
				QEndStr:         endStr,
				BucketStr:       bucket,
				MeasurementStr:  measurement,
				TagKeyStr:       tagKey,
				TagValueStr:     tagValue,
				KafkaBrokersStr: k_endpoint.bootstrapServers,
				SendTopicStr:    k_endpoint.topic,
				TotalMessages:   total_msg_count,
				StartTime:       start_time_millis, // 밀리초 단위로 변환
				EndTime:         end_time_millis,   // 밀리초 단위로 변환
				OffsetsData:     offsetsData,
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
		wg.Wait() // 모든 고루틴이 종료될 때까지 대기
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
func processJobs(client *influxdb2.Client, timeRange TimeRangeStr, q_params InfluxQueryParams, endpoint KafkaEndPoint, dropLast bool) (int, int64, int64, []string, error) {

	partitionCount, totalRecords, err := calculatePartitionAndRecords(client, timeRange, q_params, endpoint)
	if err != nil {
		return 0, 0, 0, []string{}, err
	}

	jobList, err := createJobDetailsAndList(client, timeRange, totalRecords, partitionCount, q_params, dropLast)
	if err != nil {
		return 0, 0, 0, []string{}, err
	}

	dispatcher := NewDispatcher(calculateWorkerNum(partitionCount), q_params, endpoint) // workerNum을 계산하는 함수를 호출하여 workerNum 설정
	dispatcher.Run()

	producer, startOffsets, err := setupKafkaProducerAndMetadata(endpoint)
	if err != nil {
		return 0, 0, 0, []string{}, err
	}
	defer producer.Close()

	return executeAndProcessJobs(jobList, endpoint.topic, producer, startOffsets, dispatcher)
}

func calculatePartitionAndRecords(client *influxdb2.Client, timeRange TimeRangeStr, q_params InfluxQueryParams, endpoint KafkaEndPoint) (int, int64, error) {
	partitionCount, err := getPartitionCount(endpoint)
	if err != nil {
		return 0, 0, err
	}

	timeRange.endStr = increaseOneMillisecond_str(timeRange.endStr)
	totalRecords, err := getTotalRecordCount(client, timeRange, q_params)
	if err != nil {
		return 0, 0, err
	}

	if totalRecords < int64(partitionCount) {
		return 0, 0, fmt.Errorf("totalRecords (%d) is less than partitionCount (%d)", totalRecords, partitionCount)
	}

	fmt.Printf("partitionCount: %d, totalRecords: %d\n", partitionCount, totalRecords)
	return partitionCount, totalRecords, nil
}

func createJobDetailsAndList(client *influxdb2.Client, timeRange TimeRangeStr, totalRecords int64, partitionCount int, q_params InfluxQueryParams, dropLast bool) (*JobList, error) {
	jobCount, recordsPerJob, remainingRecords := calculateJobDetails(totalRecords, partitionCount)
	fmt.Printf("Job Count: %d, Records per Job: %d, Remaining Records: %d\n", jobCount, recordsPerJob, remainingRecords)

	endTimes, err := calculateEndTimes(client, timeRange.startStr, recordsPerJob, q_params)
	if err != nil {
		return nil, err
	}

	jobList := createJobList(timeRange, endTimes, partitionCount, jobCount, remainingRecords, dropLast)
	printJobList(jobList)
	return jobList, nil
}

func setupKafkaProducerAndMetadata(endpoint KafkaEndPoint) (*kafka.Producer, map[int32]int64, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": endpoint.bootstrapServers})
	if err != nil {
		return nil, nil, err
	}

	startOffsets, err := loadKafkaMetadata(producer, endpoint.topic)
	if err != nil {
		producer.Close()
		return nil, nil, err
	}

	return producer, startOffsets, nil
}

func executeAndProcessJobs(jobList *JobList, sendTopic string, producer *kafka.Producer, startOffsets map[int32]int64, dispatcher *Dispatcher) (int, int64, int64, []string, error) {
	defer dispatcher.StopAllWorkers()

	totalMsgCount, elapsed, startTime, endTime, err := executeJobs(jobList, dispatcher)
	if err != nil {
		return 0, 0, 0, []string{}, err
	}

	endOffsets, err := loadKafkaMetadata(producer, sendTopic)
	if err != nil {
		return 0, 0, 0, []string{}, err
	}

	offsetsData := createOffsetsData(startOffsets, endOffsets)
	fmt.Println("offsetsData:")
	fmt.Println(offsetsData)

	fmt.Printf("Total processed messages: %d, Total took: %s\n", totalMsgCount, elapsed)

	return totalMsgCount, startTime.UnixNano() / 1e6, endTime.UnixNano() / 1e6, offsetsData, nil
}

func createJobList(ts TimeRangeStr, endTimes []string, partitionCount, jobCount int, remainingRecords int64, dropLast bool) *JobList {
	messagesChSize := jobCount
	if !dropLast {
		messagesChSize = jobCount + 1
	}
	messagesCh := make(chan int, messagesChSize)
	wg := new(sync.WaitGroup)

	jobList := &JobList{
		partitionCount: partitionCount,
		messagesCh:     messagesCh,
		wg:             wg,
		jobs:           []*Job{},
	}

	currentStart := ts.startStr
	for i := 0; i < jobCount; i++ {
		if i >= len(endTimes) {
			log.Fatalf("Not enough end times calculated")
		}
		endTime := endTimes[i]

		partition := i % partitionCount
		job := newJob(currentStart, endTime, partition, jobList)
		jobList.jobs = append(jobList.jobs, job)
		currentStart = endTime
	}

	if !dropLast && remainingRecords > 0 {
		fmt.Println("Handling remaining records with an extra job")
		partition := -1 // Assign this job to the next partition
		job := newJob(currentStart, increaseOneMillisecond_str(ts.endStr), partition, jobList)
		jobList.jobs = append(jobList.jobs, job)
	}
	return jobList
}

func createOffsetsData(startOffsets, endOffsets map[int32]int64) []string {
	var offsetsData []string
	for tpID, startOffset := range startOffsets {
		if endOffset, exists := endOffsets[tpID]; exists {
			offsetString := fmt.Sprintf("%d:%d:%d", tpID, startOffset, endOffset-1)
			offsetsData = append(offsetsData, offsetString)
		}
	}
	return offsetsData
}

// loadKafkaMetadata retrieves the high offsets for each partition in the specified topic.
func loadKafkaMetadata(producer *kafka.Producer, topic string) (map[int32]int64, error) {
	// Get metadata for the topic
	metadata, err := producer.GetMetadata(&topic, false, 1000)
	if err != nil {
		return nil, err
	}

	offsets := make(map[int32]int64)

	// Iterate over each partition to get the high watermark offset
	for _, partition := range metadata.Topics[topic].Partitions {
		// Use QueryWatermarkOffsets to get high offsets
		_, high, err := producer.QueryWatermarkOffsets(topic, partition.ID, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to query watermark offsets for partition %d: %v", partition.ID, err)
		}

		// Store high offsets
		offsets[partition.ID] = high
	}

	return offsets, nil
}
