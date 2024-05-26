package main

import "sync"

type Job struct {
	startStr   string
	endStr     string
	bucket     string
	measurement string
	tagKey string
	tagValue string
	eqpId      string
	sendTopic  string
	messagesCh *chan int
	wg         **sync.WaitGroup
}

func newJob( startStr, endStr, bucket, measurement, tagKey, tagValue, sendTopic string, messagesCh *chan int, wg **sync.WaitGroup) *Job {
	p := Job{
		startStr:   startStr,
		endStr:     endStr,
		bucket:     bucket,
		measurement: measurement,
		tagKey: tagKey,
		tagValue: tagValue,
		sendTopic:  sendTopic,
		messagesCh: messagesCh,
		wg:         wg,
	}
	return &p
}
