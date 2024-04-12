package main

import "time"

func divideTime(startStr, endStr string, n int) ([]struct{ Start, End string }, error) {
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
