package cloudtasks

import (
	"context"
	"time"

	tasksapi "cloud.google.com/go/cloudtasks/apiv2beta2"
	"github.com/go-kit/kit/endpoint"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/objenious/kitty"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
)

// Transport is the Cloud Tasks Transport
type Transport struct {
	gctc      *tasksapi.Client
	endpoints []*Endpoint
}

var _ kitty.Transport = &Transport{}

// NewTransport creates a new Cloud Tasks client.
func NewTransport() *Transport {
	return &Transport{}
}

// Endpoint for Cloud Tasks transport
type Endpoint struct {
	queueName string
	endpoint  endpoint.Endpoint
	maxTasks  int32
	leaseTime time.Duration
	decode    func([]byte) (interface{}, error)
}

// TaskDecoder is a function to decode task payload and return structured data
type TaskDecoder func([]byte) (interface{}, error)

// Start starts pulling msg from Cloud Tasks.
func (t *Transport) Start(ctx context.Context) error {
	var err error
	t.gctc, err = tasksapi.NewClient(ctx)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for _, e := range t.endpoints {
				msgs, err := t.leaseTasks(ctx, e)
				if err != nil {
					return err
				}
				for i := range msgs {
					err = t.process(ctx, e, msgs[i])
					if err != nil {
						return err
					}
				}
			}
		}
	}
}

// Shutdown shutdowns the cloud tasks transport
func (t *Transport) Shutdown(ctx context.Context) error {
	return t.gctc.Close()
}

// RegisterEndpoints register endpoint
func (t *Transport) RegisterEndpoints(m endpoint.Middleware) error {
	for _, e := range t.endpoints {
		e.endpoint = m(e.endpoint)
	}
	return nil
}

// Endpoint registers endpoint
func (t *Transport) Endpoint(queueName string, maxTasks int32, leaseTime time.Duration, ep endpoint.Endpoint, decode TaskDecoder) *Transport {
	e := &Endpoint{
		maxTasks:  maxTasks,
		leaseTime: leaseTime,
		queueName: queueName,
		endpoint:  ep,
		decode:    decode,
	}
	t.endpoints = append(t.endpoints, e)
	return t
}

func (t *Transport) process(ctx context.Context, e *Endpoint, msg *taskspb.Task) error {
	defer func() {
		if r := recover(); r != nil {
			err := t.nack(ctx, msg)
			_ = err // do we panic on the error
		}
	}()
	pm := msg.GetPullMessage()
	s, err := e.decode(pm.GetPayload())
	if err == nil {
		_, err = e.endpoint(ctx, s)
	}
	if err != nil {
		delay := getDelay(err)
		if delay == 0 {
			err = t.nack(ctx, msg)
			if err != nil {
				return err
			}
		} else {
			err = t.nackWithDelay(ctx, msg, delay)
			if err != nil {
				return err
			}
		}
	} else {
		err = t.ack(ctx, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Transport) leaseTasks(ctx context.Context, e *Endpoint) ([]*taskspb.Task, error) {
	tasksResp, err := t.gctc.LeaseTasks(ctx, &taskspb.LeaseTasksRequest{
		Parent:   e.queueName,
		MaxTasks: e.maxTasks,
		LeaseDuration: &duration.Duration{
			Seconds: int64(e.leaseTime.Seconds()),
		},
		ResponseView: taskspb.Task_FULL,
	})
	if tasksResp != nil {
		return tasksResp.GetTasks(), err
	}
	return nil, err
}

func (t *Transport) ack(ctx context.Context, task *taskspb.Task) error {
	return t.gctc.AcknowledgeTask(ctx, &taskspb.AcknowledgeTaskRequest{
		Name:         task.Name,
		ScheduleTime: task.ScheduleTime,
	})
}

func (t *Transport) nack(ctx context.Context, task *taskspb.Task) error {
	_, err := t.gctc.CancelLease(ctx, &taskspb.CancelLeaseRequest{
		Name:         task.Name,
		ScheduleTime: task.ScheduleTime, // this is required to make sure that our worker hold the lease
	})
	return err
}

// nackWithDelay extends the lease until it expires, so a worker can take it back to create a delayed Nack
func (t *Transport) nackWithDelay(ctx context.Context, task *taskspb.Task, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}

	_, err := t.gctc.RenewLease(ctx, &taskspb.RenewLeaseRequest{
		Name:         task.Name,
		ScheduleTime: task.ScheduleTime,
		LeaseDuration: &duration.Duration{
			Seconds: int64(delay.Seconds()),
		},
	})
	return err
}

// delayabler defines an error with a retry delay
// COMMENTED not used : à exporter pour un usage par les endpoints ?
/*type delayabler interface {
	Delay() time.Duration
}

type delaybleErr struct {
	error
	delay time.Duration
}

func delaybleError(err error, delay time.Duration) error {
	return &delaybleErr{
		error: err,
		delay: delay,
	}
}

func (err *delaybleErr) Delay() time.Duration {
	return err.delay
}*/

func getDelay(err error) time.Duration {
	type causer interface {
		Cause() error
	}

	for err != nil {
		/*if retry, ok := err.(delayabler); ok {
			return retry.Delay()
		}*/
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return 0
}
