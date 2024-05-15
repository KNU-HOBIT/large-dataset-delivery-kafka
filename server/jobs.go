package main

type Job struct {
	bucket     string
	startStr   string
	endStr     string
	eqpId      string
	messagesCh *chan int
}

func newJob(bucket, startStr, endStr, eqpId string, messagesCh *chan int) *Job {
	p := Job{bucket: bucket, startStr: startStr, endStr: endStr, eqpId: eqpId, messagesCh: messagesCh}
	return &p
}
