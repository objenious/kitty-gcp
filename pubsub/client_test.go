package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

func TestClient(t *testing.T) {
	t.Log("pubsub setup")
	ctx := context.TODO()

	psc, err := pubsub.NewClient(ctx, "project")
	if err != nil {
		t.Error("pubsub new client", err)
	}

	topic, err := psc.CreateTopic(ctx, "testclienttopic")
	if err != nil {
		t.Error("create topic", err)
	}

	sub, err := psc.CreateSubscription(ctx, "testclientsub", pubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		t.Error("create subscription", err)
	}

	ch := make(chan *pubsub.Message, 1)
	go func() {
		sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			ch <- msg
			msg.Ack()
		})
	}()

	t.Log("create client")

	c, err := NewClient(ctx, "project", topic.ID())
	if err != nil {
		t.Error("new client under test", err)
	}

	endp := c.Endpoint()

	t.Log("send data with the client, test reception")
	timer := time.NewTimer(10 * time.Second)
	for i := 0; i < 10; i++ {
		_, err = endp(ctx, fmt.Sprintf("hello world! #%d", i))
		if err != nil {
			t.Error("client endpoint", err)
		}

		timer.Reset(10 * time.Second)
		select {
		case m := <-ch:
			if m == nil {
				t.Error("no message found", err)
			}
			if fmt.Sprintf(`"hello world! #%d"`, i) != string(m.Data) {
				t.Error("message expected is different", err)
			}
		case <-timer.C:
			t.Error("timeout reached")
		}
	}
	timer.Stop()

	t.Log("pubsub clean up")
	topic.Stop()
	psc.Close()
}
