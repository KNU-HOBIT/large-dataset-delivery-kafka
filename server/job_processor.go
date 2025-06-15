package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// processJobs handles job processing for both InfluxDB and MongoDB with auto job count adjustment
func processJobs(client DatabaseClient, timeRange TimeRangeStr, params QueryParams, endpoint KafkaEndPoint, dropLast bool) (int, int64, int64, []string, error) {
	partitionCount, totalRecords, err := calculatePartitionAndRecords(client, timeRange, params, endpoint)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	jobList, err := createJobDetailsAndListWithEqualDistribution(client, totalRecords, partitionCount, params, endpoint, dropLast)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	producer, startOffsets, err := setupKafkaProducerAndMetadata(endpoint)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	defer producer.Close()

	dispatcher := NewDispatcher(config.Jobs.WorkerNum)
	dispatcher.Run(params.ConnectionID)
	defer dispatcher.StopAllWorkers()

	totalMessages, startTime, endTime, offsetsData, err := executeAndProcessJobs(jobList, endpoint.topic, producer, startOffsets, dispatcher)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	return totalMessages, startTime, endTime, offsetsData, nil
}

// calculatePartitionAndRecords calculates Kafka partition count and total records
func calculatePartitionAndRecords(client DatabaseClient, timeRange TimeRangeStr, params QueryParams, endpoint KafkaEndPoint) (int, int64, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": endpoint.bootstrapServers,
	})
	if err != nil {
		return 0, 0, err
	}
	defer producer.Close()

	metadata, err := producer.GetMetadata(&endpoint.topic, false, config.Kafka.MetadataTimeoutMs)
	if err != nil {
		return 0, 0, err
	}

	partitionCount := len(metadata.Topics[endpoint.topic].Partitions)
	totalRecords, err := client.GetTotalRecordCount(timeRange, params)
	if err != nil {
		return 0, 0, err
	}

	return partitionCount, totalRecords, nil
}

// createJobDetailsAndListWithEqualDistribution creates job list with partition-aligned job count
func createJobDetailsAndListWithEqualDistribution(client DatabaseClient, totalRecords int64, partitionCount int, params QueryParams, endpoint KafkaEndPoint, dropLast bool) (*JobList, error) {
	originalJobCount := config.Jobs.DividedJobs
	if dropLast {
		originalJobCount--
	}

	// 파티션 수에 맞춰 Job 수를 조정
	adjustedJobCount := adjustJobCountForEqualDistribution(originalJobCount, partitionCount)

	fmt.Printf("Original job count: %d, Adjusted job count: %d for %d partitions\n", originalJobCount, adjustedJobCount, partitionCount)

	// Database type에 따라 다른 Job 실행 방식 사용
	execInfos, err := client.CalculateJobExecutions(totalRecords, adjustedJobCount, params)
	if err != nil {
		return nil, err
	}

	jobList := createJobListFromExecutions(execInfos, partitionCount, params, endpoint)
	return jobList, nil
}

// adjustJobCountForEqualDistribution adjusts job count to be evenly divisible by partition count
func adjustJobCountForEqualDistribution(originalJobCount, partitionCount int) int {
	if partitionCount <= 0 {
		return originalJobCount
	}

	// 파티션 수로 나누어떨어지는 가장 가까운 수 찾기
	remainder := originalJobCount % partitionCount

	if remainder == 0 {
		// 이미 균등하게 나누어떨어짐
		return originalJobCount
	}

	// 올림과 내림 중 더 가까운 값 선택
	roundDown := originalJobCount - remainder
	roundUp := originalJobCount + (partitionCount - remainder)

	// 원래 값과의 차이가 더 작은 것 선택
	if (originalJobCount - roundDown) <= (roundUp - originalJobCount) {
		// 단, 최소 파티션 수는 보장
		if roundDown >= partitionCount {
			return roundDown
		} else {
			return roundUp
		}
	} else {
		return roundUp
	}
}

// createJobListFromExecutions creates JobList from execution info array
func createJobListFromExecutions(execInfos []JobExecutionInfo, partitionCount int, params QueryParams, endpoint KafkaEndPoint) *JobList {
	messagesCh := make(chan int, len(execInfos))
	jobs := JobList{
		partitionCount: partitionCount,
		messagesCh:     messagesCh,
		jobs:           make([]*Job, len(execInfos)),
	}

	for i := range execInfos {
		// Job 번호를 파티션 수로 나눈 나머지를 SendPartition으로 설정
		execInfos[i].SendPartition = i % partitionCount
		execInfos[i].SendTopic = endpoint.topic

		job := newJob(&jobs, params.ConnectionID, params, execInfos[i], messagesCh)
		jobs.jobs[i] = &job // append 대신 인덱스로 직접 할당
	}

	printJobList(&jobs)

	return &jobs
}

// executeAndProcessJobs executes all jobs and processes results
func executeAndProcessJobs(jobList *JobList, sendTopic string, producer *kafka.Producer, startOffsets map[int32]int64, dispatcher *Dispatcher) (int, int64, int64, []string, error) {
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
