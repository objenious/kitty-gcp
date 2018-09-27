package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/objenious/kitty"
)

var (
	testCh = make(chan interface{})
)

func testEP(_ context.Context, req interface{}) (interface{}, error) {
	if r, ok := req.(*testStruct); ok && r.Status != 0 {
		err := fmt.Errorf("status=%d", r.Status)
		testCh <- err
		return nil, err
	}
	testCh <- req
	return req, nil
}

// to launch before : gcloud beta emulators pubsub start
func TestPubSubOneTopic(t *testing.T) {
	if err := pubSubTest(t, "project", "pub", "sub"); err != nil {
		t.Error("Run pub sub test", err)
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestPubSubWithMultipleEndpoints(t *testing.T) {
	ch1 := runPubSubTest(t, "project", "pub1", "sub1")
	ch2 := runPubSubTest(t, "project", "pub2", "sub2")
	waitAndCheck(t, "pubsub 1", ch1)
	waitAndCheck(t, "pubsub 2", ch2)
}

func waitAndCheck(t *testing.T, label string, ch <-chan error) {
	select {
	case <-time.After(time.Minute):
		t.Errorf("time out in %s", label)
	case err := <-ch:
		if err != nil {
			t.Errorf("pub sub %s: %v", label, err)
		}
	}
}

func runPubSubTest(t *testing.T, projectName, topicName, subscriptionName string) <-chan error {
	ch := make(chan error)
	go func() {
		ch <- pubSubTest(t, projectName, topicName, subscriptionName)
	}()
	return ch
}

func pubSubTest(t *testing.T, projectName, topicName, subscriptionName string) error {
	ctx := context.TODO()
	c, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		t.Fatal(err)
		return err
	}
	topic := c.Topic(topicName)
	topicExists, err := topic.Exists(ctx)
	if err != nil {
		t.Fatal(err)
		return err
	}
	if !topicExists {
		topic, err = c.CreateTopic(ctx, topicName)
		if err != nil {
			t.Fatal(err)
			return err
		}
	}
	subscription := c.Subscription(subscriptionName)
	subExists, err := subscription.Exists(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !subExists {
		_, err = c.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			t.Fatal(err)
			return err
		}
	}
	err = c.Close()
	if err != nil {
		t.Fatal(err)
		return err
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
	case <-time.After(10 * time.Second):
		t.Error("Server.Run has not stopped after 10sec")
	case err := <-exitError:
		if err != nil && err != context.Canceled {
			t.Errorf("Server.Run returned an error : %s", err)
		}
	}
	if !shutdownCalled {
		t.Error("Shutdown functions are not called")
		return errors.New("Shutdown functions are not called")
	}
	return nil
}

type testStruct struct {
	Foo    string `json:"foo"`
	Status int    `json:"status"`
}

func decode(ctx context.Context, b []byte) (interface{}, error) {
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
	<-res.Ready()
}
