package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	kitendpoint "github.com/go-kit/kit/endpoint"
	"github.com/objenious/errorutil"
	"github.com/objenious/kitty"
)

// Config is the PubSubListener config
type Config struct {
	// The subscription name
	Subscription string `envconfig:"subscription"`
	// Pubsub ReceiveSettings MaxExtension
	MaxExtension time.Duration `envconfig:"max_extension" default:"30m"`
	// Pubsub ReceiveSettings MaxOutstandingMessages
	MaxOutstandingMessages int `envconfig:"max_outstanding_messages" default:"10"`
	// HealthCheckMaxAge is the maximum duration before health-check is down
	HealthCheckMaxAge time.Duration `envconfig:"healthcheck_max_age" default:"5m"`
}

// Transport is a transport that receives Records from PubSub
type Transport struct {
	c         *pubsub.Client
	endpoints []*endpoint
}

var _ kitty.Transport = &Transport{}

// Decoder is a function to decode pub/sub message and return structured data
type Decoder func([]byte) (interface{}, error)

type endpoint struct {
	pubsub           Config
	subscription     *pubsub.Subscription
	lastReceivedTime time.Time
	endpoint         kitendpoint.Endpoint
	decode           Decoder
}

// NewTransport creates a new Transport using the config from env and default dependencies
func NewTransport(ctx context.Context, projectID string) (*Transport, error) {
	c, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &Transport{c: c}, nil
}

// Endpoint create a new Endpoint using config & dependencies
func (t *Transport) Endpoint(ctx context.Context, e kitendpoint.Endpoint, decode Decoder, cfg Config) error {

	sub := t.c.Subscription(cfg.Subscription)

	if cfg.MaxExtension > 0 {
		sub.ReceiveSettings.MaxExtension = cfg.MaxExtension
	}

	if cfg.MaxOutstandingMessages > 0 {
		sub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	}

	if exists, err := sub.Exists(ctx); !exists {
		if err != nil {
			return err
		}
		return fmt.Errorf("The subscription %s does not exists", cfg.Subscription)
	}
	t.endpoints = append(t.endpoints, &endpoint{
		pubsub:           cfg,
		subscription:     sub,
		lastReceivedTime: time.Now(),
		endpoint:         e,
		decode:           decode,
	})
	return nil
}

// Close stops listening to PubSub
func (t *Transport) Close() {
	t.c.Close()
}

// Start starts listening to PubSub
func (t *Transport) Start(ctx context.Context) error {
	for _, e := range t.endpoints {
		err := e.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			e.lastReceivedTime = time.Now()

			defer func() {
				if r := recover(); r != nil {
					msg.Nack()
				}
			}()
			dec, err := e.decode(msg.Data)
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
func (t *Transport) RegisterEndpoints(m kitendpoint.Middleware) error {
	for _, e := range t.endpoints {
		e.endpoint = m(e.endpoint)
	}
	return nil
}

// Shutdown shutdowns the cloud tasks transport
func (t *Transport) Shutdown(ctx context.Context) error {
	return t.c.Close()
}
