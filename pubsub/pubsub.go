package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
	"github.com/objenious/kitty"
	"golang.org/x/sync/errgroup"
)

// Transport is a transport that receives requests from PubSub
type Transport struct {
	projectID  string
	c          *pubsub.Client
	middleware Middleware
	endpoints  []*Endpoint
}

var _ kitty.Transport = &Transport{}

// NewTransport creates a new Transport for the related Google Cloud Project
func NewTransport(ctx context.Context, projectID string) *Transport {
	return &Transport{
		projectID:  projectID,
		middleware: nopMiddleWare,
	}
}

// Endpoint creates a new Endpoint
func (t *Transport) Endpoint(subscriptionName string, endpoint endpoint.Endpoint, options ...EndpointOption) *Transport {
	e := &Endpoint{
		subscriptionName: subscriptionName,
		endpoint:         endpoint,
	}
	for _, opt := range options {
		opt(e)
	}
	t.endpoints = append(t.endpoints, e)
	return t
}

// Middleware sets a Pub/Sub middleware for all endpoint handlers
func (t *Transport) Middleware(m Middleware) *Transport {
	t.middleware = m
	return t
}

// Start starts listening to PubSub
func (t *Transport) Start(ctx context.Context) error {
	var err error
	t.c, err = pubsub.NewClient(ctx, t.projectID)
	if err != nil {
		return err
	}
	var g errgroup.Group
	for _, e := range t.endpoints {
		endpoint := e
		g.Go(func() error {
			return t.consume(ctx, endpoint)
		})
	}
	return g.Wait()
}

func (t *Transport) consume(ctx context.Context, e *Endpoint) error {
	e.subscription = t.c.Subscription(e.subscriptionName)
	exists, err := e.subscription.Exists(ctx)
	if err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("The subscription %s does not exists", e.subscriptionName)
	}
	if e.maxExtension > 0 {
		e.subscription.ReceiveSettings.MaxExtension = e.maxExtension
	}
	if e.maxOutstandingMessages > 0 {
		e.subscription.ReceiveSettings.MaxOutstandingMessages = e.maxOutstandingMessages
	}
	e.subscription.ReceiveSettings.Synchronous = e.synchronous
	if e.numGoRoutines > 0 {
		e.subscription.ReceiveSettings.NumGoroutines = e.numGoRoutines
	}
	return e.subscription.Receive(ctx, t.makeReceiveFunc(e))
}

func (t *Transport) makeReceiveFunc(e *Endpoint) func(ctx context.Context, msg *pubsub.Message) {
	handler := func(ctx context.Context, msg *pubsub.Message) error {
		PopulateRequestContext(ctx, msg)
		var (
			dec interface{}
			err error
		)
		if e.decode != nil {
			dec, err = e.decode(ctx, msg)
		} else {
			dec = msg.Data
		}
		if err == nil {
			_, err = e.endpoint(ctx, dec)
		}
		return err
	}
	handler = t.middleware(handler)
	return func(ctx context.Context, msg *pubsub.Message) {
		defer func() {
			if r := recover(); r != nil {
				msg.Nack()
			}
		}()
		err := handler(ctx, msg)
		if kitty.IsRetryable(err) {
			msg.Nack()
		} else {
			msg.Ack()
		}
	}
}

// RegisterEndpoints registers a middleware to all registered endpoints at that time
func (t *Transport) RegisterEndpoints(m endpoint.Middleware) error {
	for _, e := range t.endpoints {
		e.endpoint = m(e.endpoint)
	}
	return nil
}

// Shutdown shutdowns the google pubsub client
func (t *Transport) Shutdown(ctx context.Context) error {
	if t.c != nil {
		return t.c.Close()
	}
	return nil
}

var logKeys = map[string]interface{}{
	"pubsub-id": contextKeyID,
}

// LogKeys returns the keys for logging
func (*Transport) LogKeys() map[string]interface{} {
	return logKeys
}

// PopulateRequestContext is a RequestFunc that populates several values into
// the context from the pub/sub message. Those values may be extracted using the
// corresponding ContextKey type in this package.
func PopulateRequestContext(ctx context.Context, msg *pubsub.Message) context.Context {
	for k, v := range map[contextKey]string{
		contextKeyID: msg.ID,
	} {
		ctx = context.WithValue(ctx, k, v)
	}
	return ctx
}

type contextKey int

const (
	contextKeyID contextKey = iota
)
