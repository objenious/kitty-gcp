package pubsub

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/objenious/kitty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type msgTest struct {
	Msg string `json:"msg"`
}

var (
	testCh = make(chan interface{})
)

func testEP(_ context.Context, req interface{}) (interface{}, error) {
	if r, ok := req.(*testStruct); ok && r.Status != 0 {
		err := errors.Errorf("status=%v", r.Status)
		testCh <- err
		return nil, err
	}
	testCh <- req
	return req, nil
}

// to launch before : gcloud beta emulators pubsub start
func TestServer(t *testing.T) {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	ctx := context.TODO()
	projectName := "project"
	topicName := "pub"
	subscriptionName := "sub"
	c, err := pubsub.NewClient(ctx, projectName)
	assert.NoError(t, err)
	topic := c.Topic(topicName)
	topicExists, err := topic.Exists(ctx)
	assert.NoError(t, err)
	if !topicExists {
		topic, err = c.CreateTopic(ctx, topicName)
		assert.NoError(t, err)
	}
	subscription := c.Subscription(subscriptionName)
	subExists, err := subscription.Exists(ctx)
	assert.NoError(t, err)
	if !subExists {
		subscription, err = c.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		assert.NoError(t, err)
	}
	err = c.Close()
	assert.NoError(t, err)

	shutdownCalled := false
	ctx, cancel := context.WithCancel(context.Background())
	exitError := make(chan error)
	tr := NewTransport(ctx, "project").Endpoint(subscriptionName, testEP, func(e *Endpoint) { e.decode = decode })
	srv := kitty.NewServer(tr).Shutdown(func() {
		shutdownCalled = true
	})
	go func() {
		exitError <- srv.Run(ctx)
	}()
	for tr.c == nil {
		time.Sleep(time.Millisecond)
	}

	{
		send(ctx, topicName, tr.c, []byte(`{"foo":"bar"}`))
		if err != nil {
			t.Errorf("send to cloud tasks : %s", err)
		} else {
			resData := <-testCh
			if !reflect.DeepEqual(resData, &testStruct{Foo: "bar", Status: 0}) {
				t.Errorf("cloud tasks returned invalid data : %+v", resData)
			}
		}
	}

	cancel()
	select {
	case <-time.After(time.Second):
		t.Error("Server.Run has not stopped after 1sec")
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
func TestServerWithMultipleEndpoints(t *testing.T) {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	ctx := context.TODO()
	projectName := "project"
	topicName := "pub"
	topicName2 := "pub2"
	subscriptionName := "sub"
	subscriptionName2 := "sub2"

	c, err := pubsub.NewClient(ctx, projectName)
	assert.NoError(t, err)

	topic := c.Topic(topicName)
	topicExists, err := topic.Exists(ctx)
	assert.NoError(t, err)
	if !topicExists {
		topic, err = c.CreateTopic(ctx, topicName)
		assert.NoError(t, err)
	}

	subscription := c.Subscription(subscriptionName)
	subExists, err := subscription.Exists(ctx)
	assert.NoError(t, err)
	if !subExists {
		subscription, err = c.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		assert.NoError(t, err)
	}

	topic2 := c.Topic(topicName2)
	topicExists, err = topic2.Exists(ctx)
	assert.NoError(t, err)
	if !topicExists {
		topic2, err = c.CreateTopic(ctx, topicName2)
		assert.NoError(t, err)
	}

	subscription2 := c.Subscription(subscriptionName2)
	subExists, err = subscription2.Exists(ctx)
	assert.NoError(t, err)
	if !subExists {
		subscription2, err = c.CreateSubscription(ctx, subscriptionName2, pubsub.SubscriptionConfig{Topic: topic2})
		assert.NoError(t, err)
	}

	err = c.Close()
	assert.NoError(t, err)

	shutdownCalled := false
	ctx, cancel := context.WithCancel(context.Background())
	exitError := make(chan error)
	tr := NewTransport(ctx, "project").
		Endpoint(subscriptionName, testEP, func(e *Endpoint) { e.decode = decode }).
		Endpoint(subscriptionName2, testEP, func(e *Endpoint) { e.decode = decode })

	srv := kitty.NewServer(tr).Shutdown(func() {
		shutdownCalled = true
	})
	go func() {
		exitError <- srv.Run(ctx)
	}()
	for tr.c == nil {
		time.Sleep(time.Millisecond)
	}

	{
		send(ctx, topicName, tr.c, []byte(`{"foo":"bar"}`))
		if err != nil {
			t.Errorf("send to cloud tasks : %s", err)
		} else {
			resData := <-testCh
			if !reflect.DeepEqual(resData, &testStruct{Foo: "bar", Status: 0}) {
				t.Errorf("cloud tasks returned invalid data : %+v", resData)
			}
		}
		send(ctx, topicName2, tr.c, []byte(`{"foo":"bar"}`))
		if err != nil {
			t.Errorf("send to cloud tasks : %s", err)
		} else {
			resData := <-testCh
			if !reflect.DeepEqual(resData, &testStruct{Foo: "bar", Status: 0}) {
				t.Errorf("cloud tasks returned invalid data : %+v", resData)
			}
		}
	}

	cancel()
	select {
	case <-time.After(time.Second):
		t.Error("Server.Run has not stopped after 1sec")
	case err := <-exitError:
		if err != nil && err != context.Canceled {
			t.Errorf("Server.Run returned an error : %s", err)
		}
	}
	if !shutdownCalled {
		t.Error("Shutdown functions are not called")
	}
}

type testStruct struct {
	Foo    string `json:"foo"`
	Status int    `json:"status"`
}

func decode(b []byte) (interface{}, error) {
	d := &testStruct{}
	err := json.Unmarshal(b, d)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// send sends a message to a Cloud Tasks queue. The queue must already exist.
func send(ctx context.Context, topic string, c *pubsub.Client, data []byte) {
	t := c.Topic(topic)
	res := t.Publish(ctx, &pubsub.Message{Data: data})
	_ = <-res.Ready()
}
