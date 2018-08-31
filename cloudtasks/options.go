package cloudtasks

import "time"

// Option defines optional conf
type Option func(*Endpoint)

// DecodeOption set the decoder of the endpoint
func DecodeOption(e *Endpoint, d TaskDecoder) {
	e.decode = d
}

// LeaseTimeOption set the lease time of the endpoint
func LeaseTimeOption(e *Endpoint, d time.Duration) {
	e.leaseTime = d
}

// MaxTasksOption set the max tasks of the endpoint
func MaxTasksOption(e *Endpoint, n int32) {
	e.maxTasks = n
}
