package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var config, _ = LoadConfig("config.json")

func main() {
	// 서버 설정 및 포트 설정
	server := &http.Server{
		Addr: config.Server.Port, // 포트 3001 설정
	}

	// 라우터 설정
	handleRequests()

	// dispatcher := NewDispatcher(config.Jobs.WorkerNum) // 11개의 워커로 구성된 워커 풀 생성
	// dispatcher.Run()

	// 별도의 고루틴에서 HTTP 서버 시작
	go func() {
		// 서버 시작 시, 현재 IP와 PORT를 출력
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			log.Fatalf("Failed to get interface addresses: %v", err)
		}

		var ip string
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					ip = ipnet.IP.String()
					break
				}
			}
		}

		if ip == "" {
			log.Fatalf("Failed to get external IP address")
		}

		fmt.Printf("Server started at http://%s%s\n", ip, config.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe error: %v", err)
		}
	}()

	// 시그널 처리를 위한 채널 생성
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 시그널 대기
	<-signals
	// dispatcher.StopAllWorkers()

	// 서버 종료를 위한 새로운 컨텍스트 생성
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Server.ShutdownTimeoutSeconds)*time.Second)
	defer cancel()

	// 서버 정상 종료
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}

	fmt.Println("Server exited properly")
}
