package kittygcp

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/duration"

	"objenious/lib/log"

	"github.com/go-kit/kit/endpoint"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2beta2"
	"github.com/objenious/kitty"
	"github.com/pkg/errors"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
)

// CloudTasksTransport is the Cloud Tasks Transport
type CloudTasksTransport struct {
	googleCloudTasksClient *cloudtasks.Client
	closed                 bool
	endpoints              []*cloudtaskendpoint
}

var _ kitty.Transport = &CloudTasksTransport{}

// NewCloudTasksTransport creates a new Cloud Tasks client.
func NewCloudTasksTransport() *CloudTasksTransport {
	return &CloudTasksTransport{}
}

type cloudtaskendpoint struct {
	queueName          string
	endpoint           endpoint.Endpoint
	maxTasks           int32
	leaseTimeInSeconds int64
	decode             func([]byte) (interface{}, error)
}

// TaskDecoder is a function to decode task payload and return structured data
type TaskDecoder func([]byte) (interface{}, error)

// Start starts pulling msg from Cloud Tasks.
func (ctc *CloudTasksTransport) Start(ctx context.Context) error {
	logger := log.LoggerFromContext(ctx)
	var err error
	ctc.googleCloudTasksClient, err = cloudtasks.NewClient(ctx)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	for {
		if ctc.closed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for _, e := range ctc.endpoints {
				msgs, err := ctc.leaseTasks(ctx, e)
				if err != nil {
					logger.Err(log.LevelError, err, "get Cloud Tasks msg")
					break
				}
				for i := range msgs {
					ctc.process(ctx, e, msgs[i])
				}
			}
		}
	}
}

// Shutdown shutdowns the cloud tasks transport
func (ctc *CloudTasksTransport) Shutdown(ctx context.Context) error {
	ctc.closed = true
	return ctc.googleCloudTasksClient.Close()
}

// RegisterEndpoints register endpoint
func (ctc *CloudTasksTransport) RegisterEndpoints(m endpoint.Middleware, fn kitty.AddLoggerToContextFn) error {
	for _, e := range ctc.endpoints {
		m(e.endpoint)
	}
	return nil
}

// Endpoint registers endpoint
func (ctc *CloudTasksTransport) Endpoint(queueName string, maxTasks int32, leaseTimeInSecond int64, ep endpoint.Endpoint, decode TaskDecoder) *CloudTasksTransport {
	e := &cloudtaskendpoint{
		maxTasks:           maxTasks,
		leaseTimeInSeconds: leaseTimeInSecond,
		queueName:          queueName,
		endpoint:           ep,
		decode:             decode,
	}
	ctc.endpoints = append(ctc.endpoints, e)
	return ctc
}

func (ctc *CloudTasksTransport) process(ctx context.Context, e *cloudtaskendpoint, msg *taskspb.Task) {

	logger := log.New("task_id", msg.Name)
	ctx = log.ContextWithLogger(ctx, logger)

	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				logger.Errf(log.LevelCrit, err, "FatalError error (nack)")
			} else {
				logger.Errf(log.LevelError, errors.Errorf("%v", r), "FatalError error (nack)")
			}

			err := ctc.nack(ctx, msg)
			if err != nil {
				logger.Errf(log.LevelError, err, "FatalError error (nack) : inside recover")
			}
		}
	}()
	start := time.Now()
	pm := msg.GetPullMessage()
	data := pm.GetPayload()
	err := ctc.notifyEndpoints(ctx, data)
	if err != nil {
		delay := getDelay(err)
		if delay == 0 {
			logger.Debug("nack")
			err = ctc.nack(ctx, msg)
			if err != nil {
				logger.Errf(log.LevelCrit, err, "FatalError error (nack)")
			}
		} else {
			logger.Debug("nack delay")
			err = ctc.nackWithDelay(ctx, msg, delay)
			if err != nil {
				logger.Errf(log.LevelCrit, err, "FatalError error (nack with delay)")
			}
		}
	} else {
		err = ctc.ack(ctx, msg)
		if err != nil {
			logger.Errf(log.LevelCrit, err, "FatalError error (ack)")
		}
	}
	logger.Debugf("Request processed in %s", time.Since(start))
	if err != nil {
		logger.Err(log.LevelWarn, err, "Error processing request")
	}
}

func (ctc *CloudTasksTransport) notifyEndpoints(ctx context.Context, data []byte) error {
	for _, e := range ctc.endpoints {
		s, err := e.decode(data)
		if err != nil {
			return err
		}
		_, err = e.endpoint(ctx, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctc *CloudTasksTransport) leaseTasks(ctx context.Context, e *cloudtaskendpoint) ([]*taskspb.Task, error) {
	tasksResp, err := ctc.googleCloudTasksClient.LeaseTasks(ctx, &taskspb.LeaseTasksRequest{
		Parent:   e.queueName,
		MaxTasks: e.maxTasks,
		LeaseDuration: &duration.Duration{
			Seconds: e.leaseTimeInSeconds,
		},
		ResponseView: taskspb.Task_FULL,
	})
	return tasksResp.Tasks, errors.Wrapf(err, "getting messages from %s", e.queueName)
}

func (ctc *CloudTasksTransport) ack(ctx context.Context, t *taskspb.Task) (err error) {
	err = ctc.googleCloudTasksClient.AcknowledgeTask(ctx, &taskspb.AcknowledgeTaskRequest{
		Name:         t.Name,
		ScheduleTime: t.ScheduleTime,
	})
	return errors.Wrap(err, "ack")
}

func (ctc *CloudTasksTransport) nack(ctx context.Context, t *taskspb.Task) (err error) {
	_, err = ctc.googleCloudTasksClient.CancelLease(ctx, &taskspb.CancelLeaseRequest{
		Name:         t.Name,
		ScheduleTime: t.ScheduleTime, // this is required to make sure that our worker hold the lease
	})
	return errors.Wrap(err, "nack")
}

// nackWithDelay extends the lease until it expires, so a worker can take it back to create a delayed Nack
func (ctc *CloudTasksTransport) nackWithDelay(ctx context.Context, t *taskspb.Task, delay time.Duration) (err error) {
	if delay < time.Second {
		delay = time.Second
	}

	_, err = ctc.googleCloudTasksClient.RenewLease(ctx, &taskspb.RenewLeaseRequest{
		Name:         t.Name,
		ScheduleTime: t.ScheduleTime,
		LeaseDuration: &duration.Duration{
			Seconds: int64(delay.Seconds()),
		},
	})

	if err != nil {
		return errors.Wrap(err, "nack delay")
	}

	log.LoggerFromContext(ctx).Debug("nack with delay %s", delay.String())

	return nil
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
