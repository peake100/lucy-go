package service

import (
	"context"
	"fmt"
	"github.com/peake100/lucy-go/lucy"
	"time"
)

// progressThrottleErr is an error that results from a progress update in a
// streamProgressThrottle. Since the throttle updates are out-of-sync with requests
// from the server, we need to make sure that we capture the request number the error
// occurred for.
type progressThrottleErr struct {
	updateNum uint64
	err       error
}

// Unwrap implements xerrors.Wrapper.
func (err progressThrottleErr) Unwrap() error {
	return err
}

// Error implements builtins.error
func (err progressThrottleErr) Error() string {
	return fmt.Sprintf("error on req %v: %v", err.updateNum, err.err)
}

// streamProgressThrottle handles updating sequential progress updates to the same
// stage. It limits the number of progress updates sent to the db to 12 per second -
// enough to get a nice animated progress bar, but keeping the load on the db down for
// quickly updating jobs.
type streamProgressThrottle struct {
	// ctx is that main context for this throttle.
	ctx context.Context
	// cancelCtx cancels ctx
	cancelCtx context.CancelFunc

	// err is the last error encountered by the throttle to be returned to method
	// callers.
	err error
	// events is an internal channel to send events to our main routine.
	events chan progressEvent
	// updateNow signals the runner to immediately send the latest progress event
	// so another non-progress update can be applied.
	updateNow chan struct{}
	// updated signals back to the caller of updateNow that the handling of other
	// updates can continue.
	updated chan struct{}
	// ticker is the ticker we are going to block on for sending progress updates to the
	// db.
	ticker *time.Ticker
	// lucy is the service we should use to complete progress RPCs
	lucy *Lucy

	// latestEvent is the latest progressEvent that has been received from the caller.
	latestEvent progressEvent
	// eventPresent is whether there is an unsent event in latestEvent.
	eventPresent bool
}

// run runs the throttle.
func (throttle *streamProgressThrottle) run() {
	defer throttle.Stop()
	defer close(throttle.updated)
	defer throttle.ticker.Stop()

	for exit := false; !exit; {
		exit = throttle.handleEvent()
	}
}

// handleEvent handles a single throttle event.
func (throttle *streamProgressThrottle) handleEvent() (exit bool) {
	// update means that we should update the db.
	update := false

	// send confirmation means we should signal throttle.updated when done.
	sendConfirmation := false

	// Wait for an event
	select {
	case event := <-throttle.events:
		// If we get a new progress event, store it and mark as present
		throttle.latestEvent = event
		throttle.eventPresent = true
	case <-throttle.updateNow:
		// If we get a command to update now, mark that we need to update and then send
		// a confirmation.
		update = true
		sendConfirmation = true
	case <-throttle.ticker.C:
		// If we get a ticker proc, update.
		update = true
	case <-throttle.ctx.Done():
		// If our context has cancelled, exit immediately.
		throttle.err = throttle.ctx.Err()
		return true
	}

	// If we need tp update, and there is a waiting progress event present: update.
	if update && throttle.eventPresent {
		_, err := throttle.lucy.ProgressStage(throttle.ctx, throttle.latestEvent.update)
		if err != nil {
			// Wrap any errors in a progressThrottleErr so the original updateNum can
			// be recovered.
			throttle.err = progressThrottleErr{
				updateNum: throttle.latestEvent.updateNum,
				err:       err,
			}
			// exit and communicate that we should stop the routine.
			return true
		}
		// Set event presence to false until we get another new event.
		throttle.eventPresent = false
	}

	// If we need to confirm that the update has happened, signal.
	if sendConfirmation {
		throttle.updated <- struct{}{}
	}

	// Return to wait for the next tick.
	return false
}

// UpdateProgress queues a progress update on the throttle. This exact update may never
// occur if another call to UpdateProgress is made before the ticker fires.
func (throttle *streamProgressThrottle) UpdateProgress(
	reqNum uint64, update *lucy.ProgressStage,
) error {
	// If our main routine has encountered an error, return it.
	if throttle.err != nil {
		return throttle.err
	}

	// Otherwise send this update in.
	select {
	case throttle.events <- progressEvent{
		updateNum: reqNum,
		update:    update,
	}:
	case <-throttle.ctx.Done():
		// If our context cancels while we are doing this, exit with an error.
		return throttle.ctx.Err()
	}

	return nil
}

// Wait blocks until any outstanding progress updates are resolved. Calling this method
// signals the throttle to IMMEDIATELY apply any pending updates rather than adhering
// to the regular cadence.
//
// This method will return immediately if there are no pending progress updates.
func (throttle *streamProgressThrottle) Wait() error {
	// Send a signal to update now.
	select {
	case throttle.updateNow <- struct{}{}:
	case <-throttle.ctx.Done():
		return throttle.ctx.Err()
	}

	// Wait to get confirmation that we have updated.
	select {
	case <-throttle.updated:
	case <-throttle.ctx.Done():
		return throttle.ctx.Err()
	}

	// If the throttle encountered an error,
	if throttle.err != nil {
		return throttle.err
	}

	return nil
}

func (throttle *streamProgressThrottle) Stop() {
	// On exit, stop the ticker and close the updated channel so it doesn't block
	// forever.
	defer throttle.cancelCtx()
}

// progressEvent holds information about a progress event
type progressEvent struct {
	updateNum uint64
	update    *lucy.ProgressStage
}

// newStreamProgressThrottle creates a new *streamProgressThrottle ready for work.
func (service *Lucy) newStreamProgressThrottle(
	ctx context.Context,
) *streamProgressThrottle {
	// Create the throttle
	ctx, cancel := context.WithCancel(ctx)

	throttle := &streamProgressThrottle{
		ctx:       ctx,
		cancelCtx: cancel,
		err:       nil,
		// We don't want to buffer our channels because we don't want an updateNow
		// event to fire before all progress events have been received and evaluated.
		events:    make(chan progressEvent),
		updateNow: make(chan struct{}),
		updated:   make(chan struct{}),
		ticker:    time.NewTicker(time.Second / 12),
		lucy:      service,
	}

	// Run the event handler routine.
	go throttle.run()
	return throttle
}
