package shared

import (
	"collect_data_from_kafka/config"
	"fmt"
	"os"
)

// MessageQueue is a channel for passing messages from Kafka consumers.
var MessageQueue chan []byte
var Config *config.Config

// InitMessageQueue initializes the message queue with the specified size.
func InitMessageQueue(size int) {
	MessageQueue = make(chan []byte, size)
}

// LoadConfig loads configuration from the config package.
func LoadConfig() {
	var err error
	Config, err = config.LoadConfig()
	if err != nil {
		fmt.Println("Failed to load config:", err)
		os.Exit(1) // Exit if configuration cannot be loaded
	}
}
