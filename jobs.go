package main

type Job struct {
	startStr   string
	endStr     string
	eqpId      string
	messagesCh *chan int
}

func newJob(startStr, endStr, eqpId string, messagesCh *chan int) *Job {
	p := Job{startStr: startStr, endStr: endStr, eqpId: eqpId, messagesCh: messagesCh}
	return &p
}
