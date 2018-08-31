package cloudtasks

import "time"

// Option defines optional conf
type Option func(*Endpoint)

// DecodeOption set the decoder of the endpoint
func DecodeOption(d TaskDecoder) Option {
	return func(e *Endpoint) {
		e.decode = d
	}
}

// LeaseTimeOption set the lease time of the endpoint
func LeaseTimeOption(d time.Duration) Option {
	return func(e *Endpoint) {
		e.leaseTime = d
	}
}

// MaxTasksOption set the max tasks of the endpoint
func MaxTasksOption(n int32) Option {
	return func(e *Endpoint) {
		e.maxTasks = n
	}
}
