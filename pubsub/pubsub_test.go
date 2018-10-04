package pubsub

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/objenious/kitty"
)

var (
	testCh = make(chan interface{}, 1)
)

func testEP(_ context.Context, req interface{}) (interface{}, error) {
	if _, ok := req.(*testStruct); ok {
		testCh <- req
		return nil, nil
	}
	testCh <- nil
	return nil, nil
}

// to launch before : gcloud beta emulators pubsub start
func TestPubSubOneTopic(t *testing.T) {
	projectName := "project"
	topicName := "topic"
	subscriptionName := "subscription"
	ctx := context.TODO()

	c, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		t.Fatal(err)
		return
	}

	err = createTopicAndSubscribtion(ctx, c, topicName, subscriptionName)
	if err != nil {
		t.Fatal(err)
		return
	}

	err = c.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	shutdownCalled := false
	ctx, cancel := context.WithCancel(context.Background())
	exitError := make(chan error)
	tr := NewTransport(ctx, "project").Endpoint(subscriptionName, testEP, Decoder(decode))
	srv := kitty.NewServer(tr).Shutdown(func() {
		shutdownCalled = true
	})
	go func() {
		exitError <- srv.Run(ctx)
	}()
	for tr.c == nil {
		time.Sleep(time.Millisecond)
	}

	sendThenReceiveWithTimeout(ctx, t, tr.c, topicName)

	cancel()
	select {
	case <-time.After(10 * time.Second):
		t.Error("Server.Run has not stopped after 10sec")
	case err := <-exitError:
		if err != nil && err != context.Canceled {
			t.Errorf("Server.Run returned an error : %s", err)
		}
	}
	if !shutdownCalled {
		t.Error("Shutdown functions are not called")
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestPubSubWithMultipleEndpoints(t *testing.T) {
	projectName := "project"
	topic1Name := "topic1"
	sub1Name := "subscription1"
	topic2Name := "topic2"
	sub2Name := "subscription2"
	ctx := context.TODO()

	c, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer c.Close()

	err = createTopicAndSubscribtion(ctx, c, topic1Name, sub1Name)
	if err != nil {
		t.Fatal(err)
		return
	}

	err = createTopicAndSubscribtion(ctx, c, topic2Name, sub2Name)
	if err != nil {
		t.Fatal(err)
		return
	}

	shutdownCalled := false
	ctx, cancel := context.WithCancel(context.Background())

	tr := NewTransport(ctx, "project").
		Endpoint(sub1Name, testEP, Decoder(decode)).
		Endpoint(sub2Name, testEP, Decoder(decode))
	srv := kitty.NewServer(tr).Shutdown(func() {
		shutdownCalled = true
	})

	exitError := make(chan error)
	go func() {
		exitError <- srv.Run(ctx)
	}()

	for start := time.Now(); tr.c == nil && time.Since(start) < time.Minute; {
		// waits until pub/sub client is created in the transport, max 1 minute
		time.Sleep(time.Millisecond) // avoids too much CPU
	}
	if tr.c == nil {
		t.Error("after 1 minute, the pub/sub client is still not initialized")
		cancel()
		return
	}

	sendThenReceiveWithTimeout(ctx, t, c, topic1Name)
	sendThenReceiveWithTimeout(ctx, t, c, topic2Name)

	cancel()
	select {
	case <-time.After(10 * time.Second):
		t.Error("Server.Run has not stopped after 10sec")
	case err := <-exitError:
		if err != nil && err != context.Canceled {
			t.Errorf("Server.Run returned an error : %s", err)
		}
	}
	if !shutdownCalled {
		t.Error("Shutdown functions are not called")
	}
}

func sendThenReceiveWithTimeout(ctx context.Context, t *testing.T, c *pubsub.Client, topicName string) {
	err := send(ctx, topicName, c, []byte(`{"foo":"bar"}`))
	if err != nil {
		t.Errorf("send to topic %s: %v", topicName, err)
		return
	}
	select {
	case <-time.After(10 * time.Second):
		t.Errorf("pubsub %s no data return after timeout", topicName)
	case resData := <-testCh:
		if !reflect.DeepEqual(resData, &testStruct{Foo: "bar", Status: 0}) {
			t.Errorf("pubsub %s returned invalid data : %+v", topicName, resData)
		}
	}
}

func createTopicAndSubscribtion(ctx context.Context, c *pubsub.Client, topicName, subscriptionName string) error {
	topic, err := c.CreateTopic(ctx, topicName)
	if err != nil {
		return err
	}
	_, err = c.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
	return err
}

type testStruct struct {
	Foo    string `json:"foo"`
	Status int    `json:"status"`
}

func decode(ctx context.Context, m *pubsub.Message) (interface{}, error) {
	d := &testStruct{}
	err := json.Unmarshal(m.Data, d)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// send sends a message to Pub/Sub topic. The topic must already exist.
func send(ctx context.Context, topic string, c *pubsub.Client, data []byte) error {
	t := c.Topic(topic)
	res := t.Publish(ctx, &pubsub.Message{Data: data})
	<-res.Ready()
	_, err := res.Get(ctx)
	return err
}
