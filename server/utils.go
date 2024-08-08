package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func increaseOneNanosecond(t time.Time) time.Time {
	return t.Add(1 * time.Nanosecond)
}

func increaseOneMillisecond_str(t string) string {
	// Parse the input string using the RFC3339Nano format.
	parsedTime, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		// Handle error if the input string cannot be parsed.
		fmt.Println("Error parsing time:", err)
		return ""
	}

	// Add one nanosecond to the parsed time.
	newTime := parsedTime.Add(1 * time.Millisecond)

	// Return the new time formatted as a string using the RFC3339Nano format.
	return newTime.Format(time.RFC3339Nano)
}

// GroupRecords 함수는 records 슬라이스를 받아서 주어진 groupSize 크기만큼 묶음으로 재구성합니다.
func GroupRecords(records []interface{}, groupSize int) [][]interface{} {
	var groupedRecords [][]interface{}
	total := len(records)

	for i := 0; i < total; i += groupSize {
		end := i + groupSize
		if end > total {
			end = total
		}
		groupedRecords = append(groupedRecords, records[i:end])
	}

	return groupedRecords
}

// getPartitionCount returns the number of partitions for a given topic
// in a Kafka cluster specified by the broker address.
func getPartitionCount(topic string) (int, error) {
	// Create a new Kafka AdminClient
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": config.Kafka.BootstrapServers,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create Kafka AdminClient: %w", err)
	}
	defer adminClient.Close()

	// Retrieve metadata for the specified topic
	metadata, err := adminClient.GetMetadata(&topic, false, 5000)
	if err != nil {
		return 0, fmt.Errorf("failed to get metadata: %w", err)
	}

	// Get the topic metadata
	topicMetadata, exists := metadata.Topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	result := len(topicMetadata.Partitions)
	if result == 0 {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Return the number of partitions
	return result, nil
}

func calculateJobDetails(totalRecords int64, partitionCount int) (int, int64, int64) {
	jobCount := int64(partitionCount * config.Jobs.JobsPerPartition) // Each partition's Job count, 48
	recordsPerJob := totalRecords / jobCount
	remainingRecords := totalRecords % jobCount
	return int(jobCount), recordsPerJob, remainingRecords
}
