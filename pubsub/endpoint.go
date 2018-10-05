package pubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
)

// EndpointOption is a function to set option in endpoint
type EndpointOption func(*Endpoint)

// DecodeRequestFunc is a function to decode pub/sub message and return structured data
type DecodeRequestFunc func(context.Context, *pubsub.Message) (interface{}, error)

// Endpoint for this pubsub transport
type Endpoint struct {
	// argument
	subscriptionName string
	// options
	decode                 DecodeRequestFunc
	maxOutstandingMessages int
	maxExtension           time.Duration
	synchronous            bool
	numGoRoutines          int
	// runtime
	endpoint     endpoint.Endpoint
	subscription *pubsub.Subscription
}

// Decoder sets the decode function for requests in the endpoint
func Decoder(d DecodeRequestFunc) func(e *Endpoint) {
	return func(e *Endpoint) { e.decode = d }
}

// MaxOutstandingMessages sets the max outstanding messages
func MaxOutstandingMessages(n int) func(e *Endpoint) {
	return func(e *Endpoint) { e.maxOutstandingMessages = n }
}

// MaxExtension sets the max extension duration
func MaxExtension(d time.Duration) func(e *Endpoint) {
	return func(e *Endpoint) { e.maxExtension = d }
}

// Synchronous sets the synchronous mode
func Synchronous(b bool) func(e *Endpoint) {
	return func(e *Endpoint) { e.synchronous = b }
}

// NumGoRoutines sets the number of Go routines
func NumGoRoutines(n int) func(e *Endpoint) {
	return func(e *Endpoint) { e.numGoRoutines = n }
}
