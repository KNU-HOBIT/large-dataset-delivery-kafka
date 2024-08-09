package main

import (
	"fmt"
	"sync"
	"time"
)

type JobList struct {
	q_params       InfluxQueryParams
	sendTopic      string
	partitionCount int
	messagesCh     chan int
	wg             *sync.WaitGroup
	jobs           []*Job
}

type Job struct {
	JobList       // embedding JobList struct
	startStr      string
	endStr        string
	sendPartition int
}

func newJob(startStr, endStr string, sendPartition int, jobList *JobList) *Job {
	return &Job{
		JobList:       *jobList,
		startStr:      startStr,
		endStr:        endStr,
		sendPartition: sendPartition,
	}
}

// printJob logs the details of a single job to the console in a readable format.
func printJob(job *Job) {
	fmt.Printf("  Start Time:     %s\n", job.startStr)
	fmt.Printf("  End Time:       %s\n", job.endStr)
	fmt.Printf("  Send Partition: %d\n", job.sendPartition)
}

// printJobList logs the details of the given job list to the console in a readable format.

func printJobList(jobList *JobList) {
	fmt.Println("Job List")
	fmt.Printf("  Bucket:         %s\n", jobList.q_params.bucket)
	fmt.Printf("  Measurement:    %s\n", jobList.q_params.measurement)
	fmt.Printf("  Tag Key:        %s\n", jobList.q_params.tagKey)
	fmt.Printf("  Tag Value:      %s\n", jobList.q_params.tagValue)
	fmt.Printf("  Send Topic:     %s\n", jobList.sendTopic)
	fmt.Printf("  Messages Channel: %v\n", jobList.messagesCh)
	fmt.Printf("  Wait Group:     %v\n", jobList.wg)
	fmt.Println()
	fmt.Println("---------")
	for i, job := range jobList.jobs {
		fmt.Printf("Job #%d:\n", i+1)
		printJob(job)
	}
	fmt.Println("---------")
}

func executeJobs(jobList *JobList) (int, time.Duration, time.Time, time.Time, error) {
	startTime := time.Now()

	for _, job := range jobList.jobs {
		jobList.wg.Add(1)
		JobQueue <- *job
	}

	jobList.wg.Wait()

	endTime := time.Now()
	elapsed := endTime.Sub(startTime)
	totalMsgCount := 0
	for range jobList.jobs {
		totalMsgCount += <-jobList.messagesCh
	}
	return totalMsgCount, elapsed, startTime, endTime, nil
}
