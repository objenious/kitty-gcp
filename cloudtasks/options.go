package cloudtasks

import "time"

// EndpointOption defines optional conf
type EndpointOption func(*Endpoint)

// Decode set the decoder of the endpoint
func Decode(d TaskDecoder) EndpointOption {
	return func(e *Endpoint) {
		e.decode = d
	}
}

// LeaseTime set the lease time of the endpoint
func LeaseTime(d time.Duration) EndpointOption {
	return func(e *Endpoint) {
		e.leaseTime = d
	}
}

// MaxTasks set the max tasks of the endpoint
func MaxTasks(n int32) EndpointOption {
	return func(e *Endpoint) {
		e.maxTasks = n
	}
}
