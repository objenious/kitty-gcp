package cloudtasks

import (
	"time"

	"github.com/go-kit/kit/endpoint"
)

// TaskDecoder is a function to decode task payload and return structured data
type TaskDecoder func([]byte) (interface{}, error)

// NoOpDecoder is a function that does nothing more than return the argument
func NoOpDecoder(b []byte) (interface{}, error) {
	return b, nil
}

// Endpoint for Cloud Tasks transport
type Endpoint struct {
	queueName string
	endpoint  endpoint.Endpoint
	maxTasks  int32
	leaseTime time.Duration
	decode    TaskDecoder
}

// Endpoint registers endpoint
func (t *Transport) Endpoint(queueName string, ep endpoint.Endpoint, opts ...EndpointOption) *Transport {
	e := &Endpoint{
		queueName: queueName,
		endpoint:  ep,
		maxTasks:  1,
		leaseTime: time.Hour,
		decode:    NoOpDecoder,
	}
	for _, opt := range opts {
		opt(e)
	}
	t.endpoints = append(t.endpoints, e)
	return t
}
