package config

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	BootstrapServers    string
	ConsumerGroup       string
	KafkaTopic          string
	NumWorkers          int
	StartTimeStr        string
	EndTimeStr          string
	EquipmentID         string
	HttpRequestURLFmt   string
	MaxMessageQueueSize int
	BrokerAddressFamily string
	AutoOffsetReset     string
}

func LoadConfig() (*Config, error) {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")      // optionally look for config in the working directory
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var c Config
	if err := viper.Unmarshal(&c); err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}
	return &c, nil
}
