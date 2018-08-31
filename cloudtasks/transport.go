package cloudtasks

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	tasksapi "cloud.google.com/go/cloudtasks/apiv2beta2"
	"github.com/go-kit/kit/endpoint"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/objenious/errorutil"
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

// Start starts pulling msg from Cloud Tasks.
func (t *Transport) Start(ctx context.Context) error {
	var err error
	t.gctc, err = tasksapi.NewClient(ctx)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	g, ctx := errgroup.WithContext(ctx)
	for i := range t.endpoints {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					msgs, err := t.leaseTasks(ctx, t.endpoints[i])
					if err != nil {
						kitty.Logger(ctx).Log(err)
					}
					for i := range msgs {
						err = t.process(ctx, t.endpoints[i], msgs[i])
						if err != nil {
							kitty.Logger(ctx).Log(err)
						}
					}
				}
			}
		})
	}
	return g.Wait()
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

func (t *Transport) process(ctx context.Context, e *Endpoint, msg *taskspb.Task) error {
	defer func() {
		if r := recover(); r != nil {
			err := t.nack(ctx, msg)
			if err != nil {
				_ = kitty.Logger(ctx).Log(err)
			}
		}
	}()
	pm := msg.GetPullMessage()
	var (
		s   interface{}
		err error
	)
	if e.decode != nil {
		s, err = e.decode(pm.GetPayload())
	} else {
		s = pm.GetPayload()
	}
	if err == nil {
		_, err = e.endpoint(ctx, s)
	}
	if err != nil {
		if errorutil.IsRetryable(err) {
			delay := t.getDelay(err)
			switch delay {
			case 0:
				err = t.nack(ctx, msg)
			default:
				err = t.nackWithDelay(ctx, msg, delay)
			}
		} else {
			_ = kitty.Logger(ctx).Log(err)
			err = t.ack(ctx, msg)
		}
	} else {
		err = t.ack(ctx, msg)
	}
	return err
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

func (*Transport) getDelay(err error) time.Duration {
	type causer interface {
		Cause() error
	}

	for err != nil {
		if retry, ok := err.(Delayabler); ok {
			return retry.Delay()
		}
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return 0
}
