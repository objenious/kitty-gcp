package cloudtasks

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2beta2"
	kitendpoint "github.com/go-kit/kit/endpoint"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/objenious/kitty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
)

type msgTest struct {
	Msg string `json:"msg"`
}

var (
	testCh = make(chan interface{})
)

func testEP(_ context.Context, req interface{}) (interface{}, error) {
	if r, ok := req.(*testStruct); ok && r.Status != 0 {
		return nil, errors.Errorf("status=%v", r.Status)
	}
	testCh <- req
	return req, nil
}

const queueName = "projects/objenious-dev/locations/us-central1/queues/test-queue"

func TestServer(t *testing.T) {
	shutdownCalled := false
	ctx, cancel := context.WithCancel(context.Background())
	exitError := make(chan error)
	tr := NewTransport().
		Endpoint(queueName, testEP, DecodeOption(decode), LeaseTimeOption(time.Minute))
	srv := kitty.NewServer(tr).Shutdown(func() {
		shutdownCalled = true
	})
	go func() {
		exitError <- srv.Run(ctx)
	}()
	for tr.gctc == nil {
		time.Sleep(time.Millisecond)
	}
	{
		_, err := send(ctx, queueName, tr.gctc, []byte(`{"foo":"bar"}`), time.Millisecond)
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

func TestCloudTasksClient(t *testing.T) {
	t.Log("I want to test the Cloud Tasks client")
	{
		t.Log("Given a Cloud Tasks client")
		ctx := context.TODO()
		ch := make(chan interface{})
		tr := NewTransport().
			Endpoint(queueName, func(ctx context.Context, r interface{}) (interface{}, error) {
				ch <- r
				return nil, nil
			}, DecodeOption(decode), LeaseTimeOption(time.Minute))

		t.Log("when I send a message")
		msg := msgTest{Msg: "hello"}
		data, err := json.Marshal(msg)
		assert.NoError(t, err)
		tr.gctc, err = cloudtasks.NewClient(ctx)
		assert.NoError(t, err)
		_, err = send(ctx, queueName, tr.gctc, data, time.Minute)

		t.Log("Then, I should not get any error")
		assert.NoError(t, err)

		t.Log("And I should be able to read it")
		var c = make(chan bool, 1)

		go func() {
			err := tr.RegisterEndpoints(func(e kitendpoint.Endpoint) kitendpoint.Endpoint {
				MsgRead := &msgTest{}
				err := json.Unmarshal(data, MsgRead)
				assert.NoError(t, err)
				assert.Equal(t, "hello", MsgRead.Msg)
				c <- true // listen done => terminate test
				return nil
			})
			assert.NoError(t, err)
			err = tr.Start(ctx)
			assert.NoError(t, err)
		}()

		// In case of the message is not received, we wait 10 seconds max.
		timer := time.After(10 * time.Second)
		select {
		case <-c:
			time.Sleep(2 * time.Second) // wait for ack to be done
			// it worked
		case <-timer:
			// it did not
			t.Fatal("Message sent to cloud tasks not received")
		}
		ctx.Done()
	}
}

// send sends a message to a Cloud Tasks queue. The queue must already exist.
func send(ctx context.Context, QueueName string, ctc *cloudtasks.Client, data []byte, delay time.Duration) (*taskspb.Task, error) {
	schTime := time.Now().Add(delay)
	timestamp := timestamp.Timestamp{
		Seconds: schTime.Unix(),
	}
	req := &taskspb.CreateTaskRequest{
		Parent: queueName,
		Task: &taskspb.Task{
			ScheduleTime: &timestamp,
			PayloadType: &taskspb.Task_PullMessage{
				PullMessage: &taskspb.PullMessage{
					Payload: data,
				},
			},
		},
	}
	return ctc.CreateTask(ctx, req)
}
