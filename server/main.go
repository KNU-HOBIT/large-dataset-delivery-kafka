package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/noFlowWater/large-dataset-delivery-kafka/server/examplepb"
	"google.golang.org/protobuf/proto"
)

var config, _ = LoadConfig("config.json")

var url string = config.InfluxDB.URL
var token string = config.InfluxDB.Token

// 쿼리 range를 n개로 divide. -> n개의 Job을 비동기적으로 Worker 쓰레드로 분배.
var JobQueue = make(chan Job, config.Jobs.JobQueueCapacity)

func main() {

	example()
	// 서버 설정 및 포트 설정
	server := &http.Server{
		Addr: config.Server.Port, // 포트 3001 설정
	}

	// 라우터 설정
	handleRequests()

	dispatcher := NewDispatcher(config.Jobs.WorkerNum) // 11개의 워커로 구성된 워커 풀 생성
	dispatcher.Run()

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

func example() {
	// 메시지 생성
	person := &pb.Person{
		Id:    1234,
		Name:  "John Doe",
		Email: "jdoe@example.com",
		Phones: []*pb.Person_PhoneNumber{
			{Number: "555-4321", Type: pb.PhoneType_PHONE_TYPE_HOME},
		},
	}

	// 메시지 직렬화
	data, err := proto.Marshal(person)
	if err != nil {
		log.Fatalf("Marshaling error: %v", err)
	}

	// 직렬화된 데이터 출력
	fmt.Printf("Serialized data: %x\n", data)

	// 메시지 역직렬화
	newPerson := &pb.Person{}
	err = proto.Unmarshal(data, newPerson)
	if err != nil {
		log.Fatalf("Unmarshaling error: %v", err)
	}

	// 역직렬화된 데이터 출력
	fmt.Printf("Unmarshaled data: %v\n", newPerson)
}
