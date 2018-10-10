package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/go-kit/kit/endpoint"
	"github.com/objenious/kitty"
)

const project = "project"

func makeTestEP(resChan chan *testStruct) endpoint.Endpoint {
	return func(_ context.Context, req interface{}) (interface{}, error) {
		if r, ok := req.(*testStruct); ok {
			resChan <- r
			return nil, nil
		}
		return nil, errors.New("invalid format")
	}
}

func makeTransport(ctx context.Context, errChan chan error) *Transport {
	return NewTransport(ctx, project).Middleware(func(h Handler) Handler {
		return func(ctx context.Context, msg *pubsub.Message) error {
			err := h(ctx, msg)
			if err != nil {
				errChan <- err
			}
			return err
		}
	})
}

// to launch before : gcloud beta emulators pubsub start
func TestSingleEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := createTopicAndSub(ctx, "pub", "sub")
	if err != nil {
		t.Fatal(err)
		return
	}

	resChan := make(chan *testStruct)
	errChan := make(chan error)
	tr := makeTransport(ctx, errChan).Endpoint("sub", makeTestEP(resChan), Decoder(decode))
	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		send(ctx, "pub", tr, []byte(`{"foo":"bar"}`))
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case res := <-resChan:
			if res.Foo != "bar" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestSynchronous(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := createTopicAndSub(ctx, "syncpub", "syncsub")
	if err != nil {
		t.Fatal(err)
		return
	}

	resChan := make(chan *testStruct)
	errChan := make(chan error)
	tr := makeTransport(ctx, errChan).Endpoint("syncsub", makeTestEP(resChan), Decoder(decode), Synchronous(true))
	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		send(ctx, "syncpub", tr, []byte(`{"foo":"bar"}`))
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case res := <-resChan:
			if res.Foo != "bar" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestMultipleEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := createTopicAndSub(ctx, "mpub", "msub")
	if err != nil {
		t.Fatal(err)
		return
	}
	err = createTopicAndSub(ctx, "mpub2", "msub2")
	if err != nil {
		t.Fatal(err)
		return
	}
	errChan := make(chan error)
	resChan := make(chan *testStruct)
	res2Chan := make(chan *testStruct)
	tr := makeTransport(ctx, errChan).
		Endpoint("msub", makeTestEP(resChan), Decoder(decode)).
		Endpoint("msub2", makeTestEP(res2Chan), Decoder(decode))

	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		err := send(ctx, "mpub", tr, []byte(`{"foo":"bar"}`))
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case res := <-resChan:
			if res.Foo != "bar" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		case <-res2Chan:
			t.Error("wrong endpoint called")
		}
	}
	{
		err := send(ctx, "mpub2", tr, []byte(`{"foo":"bar2"}`))
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			t.Errorf("endpoint returned an error: %v", err)
		case <-resChan:
			t.Error("wrong endpoint called")
		case res := <-res2Chan:
			if res.Foo != "bar2" {
				t.Errorf("endpoint received invalid data: %+v", res)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := createTopicAndSub(ctx, "epub", "esub")
	if err != nil {
		t.Fatal(err)
		return
	}

	errChan := make(chan error)
	tr := makeTransport(ctx, errChan).Endpoint("esub", func(_ context.Context, req interface{}) (interface{}, error) { return nil, errors.New("foo") }, Decoder(decode))
	go func() {
		kitty.NewServer(tr).Run(ctx)
	}()

	{
		err := send(ctx, "epub", tr, []byte(`{"foo":"bar"}`))
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			if err.Error() != "foo" {
				t.Errorf("endpoint returned an invalid error: %v (should have been an endpoint error)", err)
			}
		}
	}
	{
		err := send(ctx, "epub", tr, []byte(`{"foo":1}`))
		if err != nil {
			t.Fatalf("send to pubsub : %s", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("nothing received before timeout")
		case err := <-errChan:
			if !strings.HasPrefix(err.Error(), "decode error") {
				t.Errorf("endpoint returned an invalid error: %v (should have been a decode error)", err)
			}
		}
	}
}

// to launch before : gcloud beta emulators pubsub start
func TestShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	err := createTopicAndSub(ctx, "xpub", "xsub")
	if err != nil {
		t.Fatal(err)
		return
	}
	shutdownCalled := false
	exitChan := make(chan error)
	tr := NewTransport(ctx, project).Endpoint("xsub", func(_ context.Context, req interface{}) (interface{}, error) { return nil, nil })
	go func() {
		srv := kitty.NewServer(tr).Shutdown(func() {
			shutdownCalled = true
		})
		exitChan <- srv.Run(ctx)
	}()

	cancel()
	select {
	case err := <-exitChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Server.Run returned an error : %s", err)
		}
	}
	if !shutdownCalled {
		t.Error("Shutdown functions are not called")
	}
}

type testStruct struct {
	Foo string `json:"foo"`
}

func decode(ctx context.Context, m *pubsub.Message) (interface{}, error) {
	d := &testStruct{}
	err := json.Unmarshal(m.Data, d)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	return d, nil
}

// send sends a message to Pub/Sub topic. The topic must already exist.
func send(ctx context.Context, topic string, tr *Transport, data []byte) error {
	for tr.c == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(time.Millisecond)
		}
	}
	res := tr.c.Topic(topic).Publish(ctx, &pubsub.Message{Data: data})
	_, err := res.Get(ctx)
	return err
}

func createTopicAndSub(ctx context.Context, topicName, subscriptionName string) error {
	c, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return err
	}
	topic, err := c.CreateTopic(ctx, topicName)
	if err == nil {
		_, err = c.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
	}
	if err != nil {
		return err
	}
	err = c.Close()
	return err
}
