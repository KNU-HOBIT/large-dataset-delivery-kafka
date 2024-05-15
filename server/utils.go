package main

import "time"

func divideTime(startStr, endStr string) ([]struct{ Start, End string }, error) {
	n := config.Jobs.DividedJobs
	var result []struct{ Start, End string }

	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return nil, err
	}

	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		return nil, err
	}

	duration := end.Sub(start)
	step := duration / time.Duration(n)
	for i := 0; i < n; i++ {
		newStart := start.Add(time.Duration(i) * step)
		newEnd := start.Add(time.Duration(i+1) * step)
		result = append(result, struct{ Start, End string }{newStart.Format(time.RFC3339), newEnd.Format(time.RFC3339)})
	}
	return result, nil
}

// func divideByDays(startStr, endStr string) ([]struct{ Start, End string }, error) {
// 	var result []struct{ Start, End string }

// 	start, err := time.Parse(time.RFC3339, startStr)
// 	if err != nil {
// 		return nil, err
// 	}

// 	end, err := time.Parse(time.RFC3339, endStr)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// start에서 end까지 하루 단위로 구간 나누기
// 	for current := start; current.Before(end); current = current.Add(24 * time.Hour) {
// 		newStart := current
// 		newEnd := newStart.Add(24 * time.Hour)
// 		if newEnd.After(end) {
// 			newEnd = end
// 		}
// 		result = append(result, struct{ Start, End string }{newStart.Format(time.RFC3339), newEnd.Format(time.RFC3339)})
// 	}

// 	return result, nil
// }

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
