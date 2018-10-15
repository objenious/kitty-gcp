package pubsub

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
)

// Client defines the attributes of the client
type Client struct {
	client *pubsub.Client
	topic  *pubsub.Topic
	encode EncodeRequestFunc
}

// EncodeRequestFunc defines the encode request function
type EncodeRequestFunc func(interface{}) ([]byte, error)

// NewClient creates the client
func NewClient(ctx context.Context, projectID, topic string, opts ...ClientOption) (*Client, error) {
	c, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	client := &Client{
		client: c,
		topic:  c.Topic(topic),
		encode: json.Marshal,
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// ClientOption sets an optional parameter for clients.
type ClientOption func(*Client)

// EncodeRequest sets the encode request function of the client
func EncodeRequest(er EncodeRequestFunc) ClientOption {
	return func(c *Client) {
		c.encode = er
	}
}

// SetClient sets the pubsub client
func SetClient(client *pubsub.Client) ClientOption {
	return func(c *Client) {
		c.client = client
	}
}

// do sends the message
func (c *Client) do(ctx context.Context, m interface{}) error {
	data, err := c.encode(m)
	if err != nil {
		return err
	}
	_, err = c.topic.Publish(ctx, &pubsub.Message{
		Data: data,
	}).Get(ctx)
	return err
}

// Close closes the client
func (c *Client) Close() error {
	c.topic.Stop()
	return c.client.Close()
}

// Endpoint creates an endpoint
func (c *Client) Endpoint() endpoint.Endpoint {
	return func(ctx context.Context, r interface{}) (interface{}, error) {
		return nil, c.do(ctx, r)
	}
}
