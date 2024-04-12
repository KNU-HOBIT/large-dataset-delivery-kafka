package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var url string = "http://155.230.34.51:32145"
var token string = os.Getenv("INFLUXDB_TOKEN")

const n = 30
const worker_num = 3

var topic = "iot-sensor-data-p3-r1-retention1h"

var JobQueue = make(chan Job, 100)

func main() {
	// 서버 설정 및 포트 설정
	server := &http.Server{
		Addr: ":3000", // 포트 3000 설정
	}

	var wg sync.WaitGroup

	// 라우터 설정
	handleRequests(&wg)

	dispatcher := NewDispatcher(worker_num) // 10개의 워커로 구성된 워커 풀 생성
	dispatcher.Run(&wg)

	// 별도의 고루틴에서 HTTP 서버 시작
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe error: %v", err)
		}
	}()

	// 시그널 처리를 위한 채널 생성
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 시그널 대기
	<-signals
	dispatcher.StopAllWorkers()

	// 서버 종료를 위한 새로운 컨텍스트 생성
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 서버 정상 종료
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}

	fmt.Println("Server exited properly")
}
