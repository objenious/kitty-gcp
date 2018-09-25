package pubsub

import (
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
)

// EndpointOption is a function to set option in endpoint
type EndpointOption func(*Endpoint)

// DecodeRequestFunc is a function to decode pub/sub message and return structured data
type DecodeRequestFunc func([]byte) (interface{}, error)

// Endpoint for this pubsub transport
type Endpoint struct {
	// argument
	subscriptionName string
	// options
	decode                 DecodeRequestFunc
	maxOutstandingMessages int
	maxExtension           time.Duration
	// runtime
	endpoint     endpoint.Endpoint
	subscription *pubsub.Subscription
}

// Decoder set the decode function for requests in the endpoint
func Decoder(d DecodeRequestFunc) func(e *Endpoint) {
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
