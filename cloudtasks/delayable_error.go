package cloudtasks

import "time"

// Delayabler defines an error with a retry delay
type Delayabler interface {
	Delay() time.Duration
}

// DelayableErr is an error with a delay
type DelayableErr struct {
	error
	delay time.Duration
}

// DelayableError create an delayble error: an error with a delay
func DelayableError(err error, delay time.Duration) error {
	return &DelayableErr{
		error: err,
		delay: delay,
	}
}

// Delay returns the delay of the delayable error
func (err *DelayableErr) Delay() time.Duration {
	return err.delay
}
