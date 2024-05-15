package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type ResponseData struct {
	TotalMessages int   `json:"totalMessages"`
	StartTime     int64 `json:"startTime"` // 밀리초 단위의 시작 시간 에포크
	EndTime       int64 `json:"endTime"`   // 밀리초 단위의 종료 시간 에포크
}

func handleRequests(wg *sync.WaitGroup) {
	// 라우터 설정
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// 데이터 읽기
			joblist := []*Job{}
			messagesCh := make(chan int, config.Jobs.DividedJobs)
			startStr := r.URL.Query().Get("start")
			endStr := r.URL.Query().Get("end")
			eqpId := r.URL.Query().Get("eqp_id")
			bucket := r.URL.Query().Get("bucket")

			fmt.Println("startStr:", startStr, "endStr:", endStr, "Equipment ID:", eqpId, "bucket:", bucket)

			timePairs, err := divideTime(startStr, endStr, config.Jobs.DividedJobs)
			if err != nil {
				fmt.Println("Error parsing time:", err)
				return
			}

			for _, pair := range timePairs {
				joblist = append(joblist, newJob(bucket, pair.Start, pair.End, eqpId, &messagesCh))
			}

			startTime := time.Now()

			// 작업(job) 생성 및 JobQueue에 추가
			for _, job := range joblist {
				wg.Add(1)
				JobQueue <- *job
			}

			// 모든 작업이 완료될 때까지 대기
			wg.Wait()

			// 총 처리 시간 계산
			endTime := time.Now()
			elapsed := time.Since(startTime)

			// 메시지 카운트 집계
			var total_msg_count int = 0
			for i := 0; i < len(joblist); i++ {
				total_msg_count += <-messagesCh
			}

			// 응답 데이터 구성 및 JSON 직렬화
			// 응답 데이터 구성
			responseData := ResponseData{
				TotalMessages: total_msg_count,
				StartTime:     startTime.UnixNano() / 1e6, // 밀리초 단위로 변환
				EndTime:       endTime.UnixNano() / 1e6,   // 밀리초 단위로 변환
			}
			fmt.Println(responseData)
			responseJSON, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(responseData)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// HTTP 응답 헤더 설정 및 JSON 응답 전송
			w.Header().Set("Content-Type", "application/json")
			w.Write(responseJSON)
			// 서버 콘솔에도 동일한 메시지 출력
			fmt.Printf("Total processed messages: %d, Total took: %s\n", total_msg_count, elapsed)

		case "POST":
			fmt.Fprintf(w, "post request")
		default:
			fmt.Fprintf(w, "unknown request")
		}
	})
}
