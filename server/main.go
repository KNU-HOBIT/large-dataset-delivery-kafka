package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var config, _ = LoadConfig("config.json")

var url string = config.InfluxDB.URL
var token string = config.InfluxDB.Token

var topic = config.Topics.Default

// 쿼리 range를 n개로 divide. -> n개의 Job을 비동기적으로 Worker 쓰레드로 분배.
var JobQueue = make(chan Job, config.Jobs.JobQueueCapacity)

func main() {
	// 서버 설정 및 포트 설정
	server := &http.Server{
		Addr: config.Server.Port, // 포트 3001 설정
	}

	var wg sync.WaitGroup

	// 라우터 설정
	handleRequests(&wg)

	dispatcher := NewDispatcher(config.Jobs.WorkerNum) // 11개의 워커로 구성된 워커 풀 생성
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
