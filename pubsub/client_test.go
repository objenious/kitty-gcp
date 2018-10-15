package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ps "cloud.google.com/go/pubsub"
)

func TestClient(t *testing.T) {
	t.Log("pubsub setup")
	ctx := context.TODO()
	psc, err := ps.NewClient(ctx, "project")
	require.NoError(t, err)
	topic, err := psc.CreateTopic(ctx, "testclienttopic")
	require.NoError(t, err)
	sub, err := psc.CreateSubscription(ctx, "testclientsub", ps.SubscriptionConfig{
		Topic: topic,
	})
	require.NoError(t, err)
	ch := make(chan *ps.Message, 1)
	go func() {
		sub.Receive(ctx, func(ctx context.Context, msg *ps.Message) {
			ch <- msg
			msg.Ack()
		})
	}()

	t.Log("create client")
	c, err := NewClient(ctx, "project", topic.ID())
	require.NoError(t, err)
	endp := c.Endpoint()

	t.Log("send data with the client, test reception")
	timer := time.NewTimer(10 * time.Second)
	for i := 0; i < 10; i++ {
		err = endp(ctx, fmt.Sprintf("hello world! #%d", i))
		assert.NoError(t, err)
		timer.Reset(10 * time.Second)
		select {
		case m := <-ch:
			assert.NotNil(t, m)
			assert.Equal(t, fmt.Sprintf(`"hello world! #%d"`, i), string(m.Data))
		case <-timer.C:
			t.Error("timeout reached")
		}
	}
	timer.Stop()

	t.Log("pubsub clean up")
	topic.Stop()
	psc.Close()
}
