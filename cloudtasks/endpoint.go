package cloudtasks

import (
	"time"

	"github.com/go-kit/kit/endpoint"
)

// Endpoint registers endpoint
func (t *Transport) Endpoint(queueName string, ep endpoint.Endpoint, opts ...Option) *Transport {
	e := &Endpoint{
		queueName: queueName,
		endpoint:  ep,
		maxTasks:  1,
		leaseTime: time.Hour,
	}
	for _, opt := range opts {
		opt(e)
	}
	t.endpoints = append(t.endpoints, e)
	return t
}
