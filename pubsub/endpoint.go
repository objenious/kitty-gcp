package pubsub

import (
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
)

// EndpointOption is a function to set option in endpoint
type EndpointOption func(*Endpoint)

// Decoder is a function to decode pub/sub message and return structured data
type Decoder func([]byte) (interface{}, error)

// Endpoint for this pubsub transport
type Endpoint struct {
	// argument
	subscriptionName string
	// options
	decode                 Decoder
	maxOutstandingMessages int
	maxExtension           time.Duration
	// runtime
	endpoint         endpoint.Endpoint
	subscription     *pubsub.Subscription
	lastReceivedTime time.Time
}

// Decode add a decoder in the endpoint
func Decode(d Decoder) func(e *Endpoint) {
	return func(e *Endpoint) { e.decode = d }
}

// MaxOutstandingMessages add a decoder in the endpoint
func MaxOutstandingMessages(n int) func(e *Endpoint) {
	return func(e *Endpoint) { e.maxOutstandingMessages = n }
}

// MaxExtension add a decoder in the endpoint
func MaxExtension(d time.Duration) func(e *Endpoint) {
	return func(e *Endpoint) { e.maxExtension = d }
}
