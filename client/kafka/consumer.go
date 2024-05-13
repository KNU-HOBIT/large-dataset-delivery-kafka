package kafka

import (
	"collect_data_from_kafka/shared"

	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer struct {
	consumer            *kafka.Consumer
	autoOffsetReset     string
	brokerAddressFamily string
	brokerList          string
	topic               string
	groupID             string
	run                 bool
	consume             bool
}

func NewConsumer(brokerList, topic, groupID, brokerAddressFamily, autoOffsetReset string) *KafkaConsumer {
	return &KafkaConsumer{
		autoOffsetReset:     autoOffsetReset,
		brokerAddressFamily: brokerAddressFamily,
		brokerList:          brokerList,
		topic:               topic,
		groupID:             groupID,
		run:                 true,
		consume:             false,
	}
}

func (kc *KafkaConsumer) Init(partition_id int32) error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     kc.brokerList,
		"broker.address.family": kc.brokerAddressFamily,
		"group.id":              kc.groupID,
		"auto.offset.reset":     kc.autoOffsetReset,
	})
	if err != nil {
		return err
	}
	kc.consumer = c

	kc.consumer.Assign([]kafka.TopicPartition{{Topic: &kc.topic, Partition: partition_id, Offset: kafka.OffsetEnd}})

	go kc.initConsume()

	return nil
}

func (kc *KafkaConsumer) initConsume() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	for kc.run {
		for kc.consume {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				kc.run = false
				kc.consume = false
				kc.Close()
			default:
				ev := kc.consumer.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					shared.MessageQueue <- e.Value
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
					if e.Code() == kafka.ErrAllBrokersDown {
						kc.run = false
						kc.consume = false
					}
				}
			}
		}
	}
}

func (kc *KafkaConsumer) StartConsume() error {
	fmt.Println("Starting consumer")
	kc.consume = true
	return nil
}

func (kc *KafkaConsumer) Close() {
	fmt.Println("Closing consumer")
	kc.run = false
	kc.consume = false
	kc.consumer.Close()
}

func (kc *KafkaConsumer) Consumer() *kafka.Consumer {
	return kc.consumer
}
