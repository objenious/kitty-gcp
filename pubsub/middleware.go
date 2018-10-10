package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// Handler is a function that processes a Pub/Sub message and returns an error
type Handler func(ctx context.Context, msg *pubsub.Message) error

// Middleware is a Pub/Sub middleware
type Middleware func(Handler) Handler

// nopMiddleWare is the default middleware, and does nothing.
func nopMiddleWare(h Handler) Handler {
	return h
}
