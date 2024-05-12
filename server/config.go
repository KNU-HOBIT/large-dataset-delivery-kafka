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
	InfluxDB struct {
		URL   string `json:"url"`
		Token string `json:"token"`
	} `json:"influxDB"`
	Jobs struct {
		WorkerNum        int `json:"workerNum"`
		JobQueueCapacity int `json:"jobQueueCapacity"`
		DividedJobs      int `json:"dividedJobs"`
	} `json:"jobs"`
	Topics struct {
		Default string `json:"default"`
	} `json:"topics"`
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
