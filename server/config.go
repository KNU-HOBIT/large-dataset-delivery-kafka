package main

import (
	"encoding/json"
	"os"
)

type Config struct {
	Server struct {
		Port                   string `json:"port"`
		ShutdownTimeoutSeconds int    `json:"shutdownTimeoutSeconds"`
	} `json:"server"`
	Kafka struct {
		BootstrapServers   string `json:"bootstrapServers"`
		Acks               string `json:"acks"`
		EnableIdempotence  string `json:"enableIdempotence"`
		CompressionType    string `json:"compressionType"`
		MetadataTimeoutMs  int    `json:"metadataTimeoutMs"`
		WatermarkTimeoutMs int    `json:"watermarkTimeoutMs"`
		FlushTimeoutMs     int    `json:"flushTimeoutMs"`
	} `json:"kafka"`
	Jobs struct {
		WorkerNum        int `json:"workerNum"`
		JobQueueCapacity int `json:"jobQueueCapacity"`
		DividedJobs      int `json:"dividedJobs"`
		JobsPerPartition int `json:"jobsPerPartition"`
	} `json:"jobs"`
	Database struct {
		MongoDB struct {
			MaxPoolSize                   int `json:"maxPoolSize"`
			MaxConnIdleTimeSeconds        int `json:"maxConnIdleTimeSeconds"`
			ConnectTimeoutSeconds         int `json:"connectTimeoutSeconds"`
			SocketTimeoutSeconds          int `json:"socketTimeoutSeconds"`
			ServerSelectionTimeoutSeconds int `json:"serverSelectionTimeoutSeconds"`
			ContextTimeoutSeconds         int `json:"contextTimeoutSeconds"`
			QueryTimeoutSeconds           int `json:"queryTimeoutSeconds"`
		} `json:"mongodb"`
		InfluxDB struct {
			HTTPRequestTimeoutSeconds int    `json:"httpRequestTimeoutSeconds"`
			PrecisionUnit             string `json:"precisionUnit"`
			QueryTimeoutSeconds       int    `json:"queryTimeoutSeconds"`
		} `json:"influxdb"`
	} `json:"database"`
	Formats struct {
		DateTimeFormat    string `json:"dateTimeFormat"`
		RFC3339NanoFormat string `json:"rfc3339NanoFormat"`
	} `json:"formats"`
	API struct {
		DefaultSort struct {
			MongoDB  string `json:"mongodb"`
			InfluxDB string `json:"influxdb"`
		} `json:"defaultSort"`
		SystemDatabases []string `json:"systemDatabases"`
	} `json:"api"`
	Example struct {
		PersonID    int    `json:"personId"`
		PersonName  string `json:"personName"`
		PersonEmail string `json:"personEmail"`
		PhoneNumber string `json:"phoneNumber"`
	} `json:"example"`
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
