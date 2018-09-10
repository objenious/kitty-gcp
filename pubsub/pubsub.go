package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
	"github.com/objenious/errorutil"
	"github.com/objenious/kitty"
)

// Transport is a transport that receives Records from PubSub
type Transport struct {
	projectID string
	c         *pubsub.Client
	endpoints []*Endpoint
}

var _ kitty.Transport = &Transport{}

// NewTransport creates a new Transport using the config from env and default dependencies
func NewTransport(ctx context.Context, projectID string) *Transport {
	return &Transport{
		projectID: projectID,
	}
}

// Endpoint create a new Endpoint using config & dependencies
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

// Close stops listening to PubSub
func (t *Transport) Close() {
	t.c.Close()
}

// Start starts listening to PubSub
func (t *Transport) Start(ctx context.Context) error {
	var err error
	t.c, err = pubsub.NewClient(ctx, t.projectID)
	if err != nil {
		return err
	}
	for _, e := range t.endpoints {
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
		err = e.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			defer func() {
				if r := recover(); r != nil {
					msg.Nack()
				}
			}()
			var (
				dec interface{}
				err error
			)
			if e.decode != nil {
				dec, err = e.decode(msg.Data)
			} else {
				dec = msg.Data
			}
			if err == nil {
				_, err = e.endpoint(ctx, dec)
			}
			if errorutil.IsRetryable(err) {
				msg.Nack()
			} else {
				msg.Ack()
			}
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// RegisterEndpoints register endpoint
func (t *Transport) RegisterEndpoints(m endpoint.Middleware) error {
	for _, e := range t.endpoints {
		e.endpoint = m(e.endpoint)
	}
	return nil
}

// Shutdown shutdowns the cloud tasks transport
func (t *Transport) Shutdown(ctx context.Context) error {
	return t.c.Close()
}
