package main

import (
	"encoding/json"
	"os"
)

type Config struct {
	Server struct {
		Port string `json:"port"`
	} `json:"server"`
	Kafka struct {
		BootstrapServers  string `json:"bootstrapServers"`
		Acks              string `json:"acks"`
		EnableIdempotence string `json:"enableIdempotence"`
		CompressionType   string `json:"compressionType"`
	} `json:"kafka"`
	Jobs struct {
		WorkerNum        int `json:"workerNum"`
		JobQueueCapacity int `json:"jobQueueCapacity"`
		DividedJobs      int `json:"dividedJobs"`
		JobsPerPartition int `json:"jobsPerPartition"`
	} `json:"jobs"`
}

func LoadConfig(filename string) (Config, error) {
	var config Config
	configFile, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	if err != nil {
		return config, err
	}
	return config, nil
}
