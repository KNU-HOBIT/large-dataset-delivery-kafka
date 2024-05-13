package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"collect_data_from_kafka/kafka"
	"collect_data_from_kafka/myhttp"
	"collect_data_from_kafka/shared"
	"collect_data_from_kafka/util"
)

func main() {

	// Setup signal handling
	util.SetupSignalHandling()
	// Load configuration and setup the message queue
	shared.LoadConfig()
	// Initialize the message queue
	shared.InitMessageQueue(shared.Config.MaxMessageQueueSize)

	var wg sync.WaitGroup

	for i := 0; i < shared.Config.NumWorkers; i++ {
		wg.Add(1)
		go kafkaConsumerWorker(int32(i))
	}
	// Start HTTP request handling based on user input
	reader := bufio.NewReader(os.Stdin) // bufio.Reader를 사용하여 입력을 처리
	fmt.Println("Press Enter to send HTTP request, or type 'exit' to quit:")
	userInput, err := reader.ReadString('\n') // '\n'을 기준으로 사용자 입력을 읽음
	if err != nil {
		fmt.Println("Error reading input:", err)
		return
	}
	userInput = strings.TrimSpace(userInput) // 입력에서 공백 및 줄바꿈 문자 제거
	if userInput == "exit" {
		return
	} else if userInput == "" { // Enter만 친 경우
		startTime := time.Now()
		response, err := myhttp.SendHTTPRequest(shared.Config.HttpRequestURLFmt, shared.Config.StartTimeStr, shared.Config.EndTimeStr, shared.Config.EquipmentID)
		if err != nil {
			fmt.Println("Failed to send HTTP request:", err)
			return
		}

		totalMessages := response.TotalMessages
		sendStartTime := response.StartTime
		sendEndTime := response.EndTime

		collectMessages(totalMessages)

		elapsedTime := time.Since(startTime)

		fmt.Println("Data collection complete.")
		fmt.Println("Start :", shared.Config.StartTimeStr)
		fmt.Println("End :", shared.Config.EndTimeStr)
		fmt.Printf("Total messages to collect: %d\n", totalMessages)
		fmt.Printf("messages to send ( Start , End ): %d, %d\n", sendStartTime, sendEndTime)
		fmt.Printf("Elapsed time for HTTP request and message processing: %v\n", elapsedTime)
	}
	wg.Wait()
	fmt.Println("All workers have been terminated.")
}

func kafkaConsumerWorker(partition_id int32) {
	consumer := kafka.NewConsumer(shared.Config.BootstrapServers, shared.Config.KafkaTopic, shared.Config.ConsumerGroup, shared.Config.BrokerAddressFamily, shared.Config.AutoOffsetReset)
	consumer.Init(partition_id)
	consumer.StartConsume()
	fmt.Println(consumer.Consumer().String())
}

func collectMessages(totalMessages int) {
	var collectedData [][]byte   // Store slices of bytes directly
	var firstRecordSize int      // To store the size of the first record
	var firstRecordMeasured bool // Boolean to check if the first record size has been recorded

	cnt := 0

	// 새 go루틴을 시작하여 3초마다 cnt 값 출력
	go func() {
		for {
			fmt.Printf("Current count: %d\n", cnt)
			time.Sleep(3 * time.Second)
		}
	}()

	for cnt < totalMessages {
		msg := <-shared.MessageQueue // Directly receive from the message queue
		collectedData = append(collectedData, msg)
		if !firstRecordMeasured { // Check if the first record size has been recorded
			firstRecordSize = len(msg) // Store the size of the first record
			firstRecordMeasured = true // Set the flag to true after recording the size
		}
		cnt++
	}

	// Calculate the estimated total size based on the first record size
	recordsCollected := len(collectedData)
	estimatedTotalSize := firstRecordSize * recordsCollected

	fmt.Printf("record_num: %d\n", recordsCollected)
	fmt.Printf("Total size of collected data: %d bytes (estimated)\n", estimatedTotalSize)

	// Optionally, return collectedData if needed
	// return collectedData
}
