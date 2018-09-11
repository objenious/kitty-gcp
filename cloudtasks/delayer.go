package cloudtasks

import "time"

// Delayer defines an error with a retry delay
type Delayer interface {
	Delay() time.Duration
}

type delayerErr struct {
	error
	delay time.Duration
}

// DelayerError create an delayer error: an error with a delay
func DelayerError(err error, delay time.Duration) error {
	return &delayerErr{
		error: err,
		delay: delay,
	}
}

// Delay returns the delay of the delayable error
func (err *delayerErr) Delay() time.Duration {
	return err.delay
}
