package myhttp

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type HTTPResponse struct {
	TotalMessages int   `json:"totalMessages"`
	StartTime     int64 `json:"startTime"` // Start time in milliseconds epoch format
	EndTime       int64 `json:"endTime"`   // End time in milliseconds epoch format
}

func SendHTTPRequest(urlFmt, start, end, eqpID string) (*HTTPResponse, error) {
	url := fmt.Sprintf(urlFmt, start, end, eqpID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-OK HTTP status: %s", resp.Status)
	}

	var result HTTPResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
