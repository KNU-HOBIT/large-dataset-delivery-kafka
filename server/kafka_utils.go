package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// setupKafkaProducerAndMetadata creates Kafka producer and loads initial metadata
func setupKafkaProducerAndMetadata(endpoint KafkaEndPoint) (*kafka.Producer, map[int32]int64, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  endpoint.bootstrapServers,
		"acks":               config.Kafka.Acks,
		"enable.idempotence": config.Kafka.EnableIdempotence,
		"compression.type":   config.Kafka.CompressionType,
	})
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

// loadKafkaMetadata loads metadata and offsets for a Kafka topic
func loadKafkaMetadata(producer *kafka.Producer, topic string) (map[int32]int64, error) {
	// Get metadata for the topic
	metadata, err := producer.GetMetadata(&topic, false, config.Kafka.WatermarkTimeoutMs)
	if err != nil {
		return nil, err
	}

	offsets := make(map[int32]int64)

	// Iterate over each partition to get the high watermark offset
	for _, partition := range metadata.Topics[topic].Partitions {
		// Use QueryWatermarkOffsets to get high offsets
		_, high, err := producer.QueryWatermarkOffsets(topic, partition.ID, config.Kafka.WatermarkTimeoutMs)
		if err != nil {
			return nil, fmt.Errorf("failed to query watermark offsets for partition %d: %v", partition.ID, err)
		}

		// Store high offsets
		offsets[partition.ID] = high
	}

	return offsets, nil
}

// createOffsetsData creates human-readable offset information
func createOffsetsData(startOffsets, endOffsets map[int32]int64) []string {
	var offsetsData []string
	for tpID, startOffset := range startOffsets {
		if endOffset, exists := endOffsets[tpID]; exists {
			offsetString := fmt.Sprintf("%d:%d:%d", tpID, startOffset, endOffset-1)
			offsetsData = append(offsetsData, offsetString)
			fmt.Printf("Partition %d: %d -> %d (%d messages)\n", tpID, startOffset, endOffset-1, endOffset-startOffset)
		}
	}
	return offsetsData
}
