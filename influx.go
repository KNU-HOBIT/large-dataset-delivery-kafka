package main

import (
	"context"

	"fmt"
	"log"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	jsoniter "github.com/json-iterator/go"
)

func ReadData(client *influxdb2.Client, start string, end string, eqpId string) []interface{} {
	org := "influxdata"
	queryAPI := (*client).QueryAPI(org)

	// "payload" 키를 조건으로 검색하는 query 문장
	query :=
		`
	from(bucket: "hobit_iot_sensor")
    |> range(start: ` + start + `, stop: ` + end + `)
    // |> filter(fn: (r) => r["eqp_id"] == "` + eqpId + `")
	`

	// fmt.Println(query)

	// |> filter(fn: (r) => r["eqp_id"] == )
	startTime := time.Now()
	results, err := queryAPI.Query(context.Background(), query)
	elapsed := time.Since(startTime)
	fmt.Printf("client: %x Query took %s\n", client, elapsed)

	if err != nil {
		log.Fatal(err)
	}

	var jsonObjects []interface{} // JSON 객체들을 저장할 슬라이스

	for results.Next() {
		record := results.Record()

		// _value 필드 추출 및 파싱
		if v, ok := record.Values()["_value"].(string); ok {

			var jsonObj interface{}
			if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(v), &jsonObj); err != nil {
				log.Printf("Error parsissssng JSON from _value: %v", err)
				continue // 파싱에 실패하면 이 레코드는 건너뜁니다
			}
			// 파싱된 JSON 객체를 슬라이스에 추가
			jsonObjects = append(jsonObjects, jsonObj)
		}
	}
	if err := results.Err(); err != nil {
		log.Fatal(err)
	}

	return jsonObjects
}
