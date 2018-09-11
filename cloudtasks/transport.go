package cloudtasks

import (
	"context"
	"strings"
	"time"

	tasksapi "cloud.google.com/go/cloudtasks/apiv2beta2"
	"github.com/go-kit/kit/endpoint"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/kelseyhightower/envconfig"
	"github.com/objenious/kitty"
	"golang.org/x/sync/errgroup"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
)

// TransportConfig is the configuration of Transport
type TransportConfig struct {
	CheckContextPeriodInMilliseconds int `default:"1000" envconfig:"check_context_period_ms"`
}

// Transport is the Cloud Tasks Transport
type Transport struct {
	cfg       TransportConfig
	gctc      *tasksapi.Client
	endpoints []*Endpoint
}

var _ kitty.Transport = &Transport{}

// NewTransportWithConfig creates a new Cloud Tasks client.
func NewTransportWithConfig(cfg TransportConfig) *Transport {
	return &Transport{cfg: cfg}
}

// NewTransport create a new Cloud Tasks client.
func NewTransport() *Transport {
	cfg := TransportConfig{}
	err := envconfig.Process("cloudtasks", &cfg)
	if err != nil {
		panic(err)
	}
	return NewTransportWithConfig(cfg)
}

// Start starts pulling msg from Cloud Tasks.
func (t *Transport) Start(ctx context.Context) error {
	var err error
	t.gctc, err = tasksapi.NewClient(ctx)
	if err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)

	for _, e := range t.endpoints {
		g.Go(t.consumer(ctx, e))
	}
	return g.Wait()
}

func (t *Transport) consumer(ctx context.Context, e *Endpoint) func() error {
	return func() error {
		ticker := time.NewTicker(time.Duration(t.cfg.CheckContextPeriodInMilliseconds) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				msgs, err := t.leaseTasks(ctx, e)
				if err != nil {
					kitty.Logger(ctx).Log(err)
				}
				for i := range msgs {
					taskCtx := PopulateRequestContext(ctx, msgs[i])
					_ = t.process(taskCtx, e, msgs[i])
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
	s, err = e.decode(pm.GetPayload())
	if err == nil {
		_, err = e.endpoint(ctx, s)
	}
	if err != nil {
		if kitty.IsRetryable(err) {
			delay := t.getDelay(err)
			switch delay {
			case 0:
				err = t.nack(ctx, msg)
			default:
				err = t.nackWithDelay(ctx, msg, delay)
			}
		} else {
			_ = t.ack(ctx, msg)
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
	if err != nil {
		return nil, err
	}
	return tasksResp.GetTasks(), nil
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
		if retry, ok := err.(Delayer); ok {
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

var logKeys = map[string]interface{}{
	"cloudtasks-task-id":  nil,
	"cloudtasks-queue-id": nil,
}

// LogKeys returns the keys for logging
func (*Transport) LogKeys() map[string]interface{} {
	return logKeys
}

// PopulateRequestContext is a RequestFunc that populates several values into
// the context from the HTTP request. Those values may be extracted using the
// corresponding ContextKey type in this package.
func PopulateRequestContext(ctx context.Context, t *taskspb.Task) context.Context {
	for k, v := range map[contextKey]string{
		ContextKeyTaskID:  taskID(t),
		ContextKeyQueueID: queueID(t),
	} {
		ctx = context.WithValue(ctx, k, v)
	}
	return ctx
}

func taskID(t *taskspb.Task) string {
	return extractValueFromPath(t.GetName(), "tasks")
}

func queueID(t *taskspb.Task) string {
	return extractValueFromPath(t.GetName(), "queues")
}

func extractValueFromPath(path string, key string) string {
	key = "/" + key + "/"
	idx := strings.Index(path, key)
	if idx > 0 {
		res := path[idx+len(key):]
		idx = strings.Index(res, "/")
		if idx > 0 {
			res = res[:idx]
		}
		return res
	}
	return "???"
}

type contextKey int

const (
	// ContextKeyTaskID is populated in the context by
	// PopulateRequestContext. Its value is t.ID.
	ContextKeyTaskID contextKey = iota

	// ContextKeyQueueID is populated in the context by
	// PopulateRequestContext. Its value is t.QueueID.
	ContextKeyQueueID
)