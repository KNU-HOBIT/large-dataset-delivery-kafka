package main

import (
	"fmt"
	"time"
)

type JobList struct {
	partitionCount int
	messagesCh     chan int
	jobs           []*Job
}

type Job struct {
	jobList      *JobList
	connectionID string
	params       QueryParams
	execInfo     JobExecutionInfo // startStr, endStr 대신 JobExecutionInfo 사용
	messagesCh   chan int
}

// JobExecutionInfo holds job execution parameters
type JobExecutionInfo struct {
	// Time-based execution (for InfluxDB)
	StartStr string
	EndStr   string

	// Offset-based execution (for MongoDB)
	StartOffset int64
	EndOffset   int64

	// Common fields
	SendPartition int
	SendTopic     string
}

func newJob(jobList *JobList, connectionID string, params QueryParams, execInfo JobExecutionInfo, messagesCh chan int) Job {
	return Job{
		jobList:      jobList,
		connectionID: connectionID,
		params:       params,
		execInfo:     execInfo,
		messagesCh:   messagesCh,
	}
}

// printJob logs the details of a single job to the console in a readable format.
func printJob(job *Job) {
	if job == nil {
		fmt.Printf("  Job is nil\n")
		return
	}
	if job.params.DBType == "influx" {
		fmt.Printf("  Start Time:     %s\n", job.execInfo.StartStr)
		fmt.Printf("  End Time:       %s\n", job.execInfo.EndStr)
	} else if job.params.DBType == "mongo" {
		fmt.Printf("  Start Offset:   %d\n", job.execInfo.StartOffset)
		fmt.Printf("  End Offset:     %d\n", job.execInfo.EndOffset)
	}
	fmt.Printf("  Send Partition: %d\n", job.execInfo.SendPartition)
}

// printJobList logs the details of the given job list to the console in a readable format.

func printJobList(jobList *JobList) {
	fmt.Println("Job List")
	fmt.Printf("  Messages Channel: %v\n", jobList.messagesCh)
	fmt.Println()
	fmt.Println("---------")
	for i, job := range jobList.jobs {
		fmt.Printf("Job #%d:\n", i+1)
		printJob(job)
	}
	fmt.Println("---------")
}

func executeJobs(jobList *JobList, dispatcher *Dispatcher) (int, time.Duration, time.Time, time.Time, error) {
	startTime := time.Now()

	for _, job := range jobList.jobs {
		dispatcher.SubmitJob(job)
	}

	dispatcher.WaitForAllJobs()

	endTime := time.Now()
	elapsed := endTime.Sub(startTime)
	totalMsgCount := 0
	for range jobList.jobs {
		totalMsgCount += <-jobList.messagesCh
	}
	return totalMsgCount, elapsed, startTime, endTime, nil
}
