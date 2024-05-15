package main

import "sync"

type Job struct {
	bucket     string
	startStr   string
	endStr     string
	eqpId      string
	sendTopic  string
	messagesCh *chan int
	wg         **sync.WaitGroup
}

func newJob(bucket, startStr, endStr, eqpId, sendTopic string, messagesCh *chan int, wg **sync.WaitGroup) *Job {
	p := Job{
		bucket:     bucket,
		startStr:   startStr,
		endStr:     endStr,
		eqpId:      eqpId,
		sendTopic:  sendTopic,
		messagesCh: messagesCh,
		wg:         wg,
	}
	return &p
}
