package service

import (
	"errors"
	"fmt"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/lucy"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
)

// errClientClosed is a sentinel error to indicate the runner is should exit without
// error.
var errClientClosed = errors.New("runner stream closed by user")

// runnerState keeps the current state of the runner for passing around helper
// functions.
type runnerState struct {
	// CurrentStageId is the last sent lucy.StageID from the caller.
	CurrentStageId *lucy.StageID
	// CurrentUpdate is the current update being handled.
	CurrentUpdate *lucy.RunnerUpdate
	// UpdateNum the counter identifying the current update.
	UpdateNum uint64
	// ProgressThrottle is the streamProgressThrottle handling stream updates for this
	// runner.
	ProgressThrottle *streamProgressThrottle
}

// Runner implements lucy.LucyServer.
func (service *Lucy) Runner(server lucy.Lucy_RunnerServer) (err error) {
	// Set up our initial state
	state := runnerState{
		ProgressThrottle: service.newStreamProgressThrottle(server.Context()),
	}

	// Catch and pack errors.
	defer func() {
		err = service.packRunnerError(err, state.UpdateNum)
	}()

	// Receive our first update.
	state, err = service.runnerReceiveFirstUpdate(server, state)

	// Receive updates.
	for {
		// Handle an error from receiving an update.
		if err != nil {
			return err
		}

		// Handle the update.
		state, err = service.runnerHandleUpdate(server, state)

		// Increment the updateNum and receive the next one.
		state.UpdateNum++
		state.CurrentUpdate, err = receiveRunnerUpdate(server)
	}
}

// runnerHandleUpdate handles a single update.
func (service *Lucy) runnerHandleUpdate(
	server lucy.Lucy_RunnerServer, state runnerState,
) (runnerState, error) {
	var newId bool
	state, newId = updateStageId(state)

	// If this update ID is not the same as our last id or this is not progress
	// stage, force a progress update to make sure updates don't happen
	// out-of-order.
	_, isProgress := state.CurrentUpdate.Update.(*lucy.RunnerUpdate_Progress)
	if newId || !isProgress {
		if err := state.ProgressThrottle.Wait(); err != nil {
			return state, err
		}
	}

	// Apply the update to the DB.
	err := service.applyRunnerUpdate(server, state)
	return state, err
}

// runnerReceiveFirstUpdate handles receiving the first message and creating our runner
// state value.
func (service *Lucy) runnerReceiveFirstUpdate(
	server lucy.Lucy_RunnerServer, state runnerState,
) (runnerState, error) {
	// Receive the first update.
	var err error
	state.CurrentUpdate, err = receiveRunnerUpdate(server)

	// The first update MUST have a stage id, so error out if it does not.
	if err == nil && state.CurrentUpdate.StageId == nil {
		return state, service.errs.NewErr(
			pkerr.ErrValidation,
			"first RunnerUpdate must contain non-nil StageID",
			nil,
			nil,
		)
	}

	// Set the current stage id to the update's stage id/
	state.CurrentStageId = state.CurrentUpdate.StageId

	// This error will be handled by the caller, if it occurred.
	return state, err
}

// receiveRunnerUpdate receives an update from the stream and does some error handling.
func receiveRunnerUpdate(server lucy.Lucy_RunnerServer) (*lucy.RunnerUpdate, error) {
	update, err := server.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errClientClosed
		}
		return nil, fmt.Errorf("error receiving update: %w", err)
	}

	return update, nil
}

// updateStageId checks whether this update is a new id.
//
// if clearProgress is returned as true, the update throttle should be cleared because
// we are now either handling requests for a new id, or a non-progress update.
func updateStageId(in runnerState) (out runnerState, newId bool) {
	hasId := in.CurrentUpdate.StageId != nil
	newId = hasId && !in.CurrentStageId.Eq(in.CurrentUpdate.StageId)

	// Replace the current StageId we are working on if one was provided.
	if hasId {
		in.CurrentStageId = in.CurrentUpdate.StageId
	}

	return in, newId
}

// applyRunnerUpdate takes the current runner state and applies the update to the DB.
func (service *Lucy) applyRunnerUpdate(
	server lucy.Lucy_RunnerServer,
	state runnerState,
) (err error) {
	// Determine what kind of update to run. We can call the Unary RPCs
	// implementations for these updates.
	switch updateInfo := state.CurrentUpdate.Update.(type) {
	case *lucy.RunnerUpdate_Start:
		thisUpdate := &lucy.StartStage{
			StageId: state.CurrentStageId,
			Update:  updateInfo.Start,
		}
		_, err = service.StartStage(server.Context(), thisUpdate)
	case *lucy.RunnerUpdate_Progress:
		thisUpdate := &lucy.ProgressStage{
			StageId: state.CurrentStageId,
			Update:  updateInfo.Progress,
		}
		// Progress updates get handled through the throttle to limit super chatty
		// workers from overloading the database.
		err = state.ProgressThrottle.UpdateProgress(state.UpdateNum, thisUpdate)
	case *lucy.RunnerUpdate_Complete:
		thisUpdate := &lucy.CompleteStage{
			StageId: state.CurrentStageId,
			Update:  updateInfo.Complete,
		}
		_, err = service.CompleteStage(server.Context(), thisUpdate)
	case *lucy.RunnerUpdate_Confirm:
		// On a confirm, send back an empty response.
		err = server.Send(emptyResponse)
	default:
		panic(fmt.Errorf(
			"received unexpected update of type %T", updateInfo,
		))
	}

	return err
}

// packRunnerError converts outgoing errors to APIErrors and adds a detail of the
// updateNumber that caused the error.
func (service *Lucy) packRunnerError(err error, updateNum uint64) error {
	if err == nil {
		return nil
	} else if errors.Is(err, errClientClosed) {
		return nil
	}

	// If this is a throttle error, extract the actual error and request number.
	if throttleErr, ok := err.(progressThrottleErr); ok {
		err = throttleErr.err
		updateNum = throttleErr.updateNum
	}

	detail, packErr := anypb.New(&lucy.RunnerErrorDetails{UpdateNum: updateNum})
	if packErr != nil {
		return packErr
	}

	var apiErr pkerr.APIError

	switch errType := err.(type) {
	case pkerr.APIError:
		apiErr = errType
	case *pkerr.SentinelError:
		apiErr = service.errs.NewErr(
			errType,
			"",
			nil,
			err,
		).(pkerr.APIError)
	default:
		apiErr = service.errs.NewErr(
			pkerr.ErrUnknown,
			"",
			nil,
			err,
		).(pkerr.APIError)
	}

	apiErr.Proto.Details = append(apiErr.Proto.Details, detail)
	return apiErr
}
