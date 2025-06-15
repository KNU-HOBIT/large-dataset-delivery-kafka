package main

import (
	"time"
)

func increaseOneNanosecond(t time.Time) time.Time {
	return t.Add(1 * time.Nanosecond)
}

// GroupRecords 함수는 records 슬라이스를 받아서 주어진 groupSize 크기만큼 묶음으로 재구성합니다.
func GroupRecords(records []interface{}, groupSize int) [][]interface{} {
	var groupedRecords [][]interface{}
	total := len(records)

	for i := 0; i < total; i += groupSize {
		end := i + groupSize
		if end > total {
			end = total
		}
		groupedRecords = append(groupedRecords, records[i:end])
	}

	return groupedRecords
}
