package lucy

import (
	"context"
	"errors"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"google.golang.org/protobuf/proto"
)

// Worker is an interface for running a job.
type Worker interface {
	// Setup sets up a worker to run.
	Setup(jobData proto.Message) error
	// RunStage runs a specific job stage, returning the stage result or an error.
	//
	// progress is a channel for sending progress updates to the job tracking service.
	RunStage(
		ctx context.Context, stage *JobStage, progress chan<- float32,
	) StageResult
}

// StageResult is the result of a stage run.
type StageResult struct {
	// Data is the result data to be applied to the stage if the result is a success.
	Data proto.Message
	// Requeue is whether to requeue the job for retry on a a failure.
	Requeue bool
	// Err holds any errors that occurred.
	Err error
}

// JobResult holds the result of a job run
type JobResult struct {
	Requeue bool
	Err     error
}

// JobRunner runs jobs using a LucyClient.
type JobRunner struct {
	name   string
	client LucyClient
}

// RunJob runs a job
func (runner JobRunner) RunJob(
	ctx context.Context, jobId *cereal.UUID, worker Worker,
) (result JobResult) {
	job, err := runner.client.GetJob(ctx, jobId)
	if err != nil {
		result.Err = fmt.Errorf("error getting job: %w", err)
		return result
	}

	jobData, err := job.Input.UnmarshalNew()
	if err != nil {
		result.Err = fmt.Errorf("error unmarshalling input data: %w", err)
		return result
	}

	// Run the worker setup.
	if err = worker.Setup(jobData); err != nil {
		result.Err = fmt.Errorf("error setting up job: %w", err)
		return result
	}

	// Get the update stream
	updateStream, err := runner.client.Runner(ctx)
	if err != nil {
		result.Err = fmt.Errorf("error sening upate to stream")
		return result
	}

	// Iterate over the stages and run them.
	for i, stage := range job.Stages {
		stageId := &StageID{
			JobId:      job.Id,
			StageIndex: int32(i),
		}

		stageResult := runner.runStage(ctx, updateStream, stageId, stage, worker)
		if stageResult.Err != nil {
			result.Err = stageResult.Err
			result.Requeue = stageResult.Requeue
			return result
		}
	}

	return result
}

func (runner *JobRunner) runStage(
	ctx context.Context,
	stream Lucy_RunnerClient,
	id *StageID,
	stage *JobStage,
	worker Worker,
) (result StageResult) {
	// Send the stage start update
	start := &RunnerUpdate{
		StageId: id,
		Update: &RunnerUpdate_Start{
			Start: &StartStageUpdate{
				RunBy: runner.name,
			},
		},
		Confirm: true,
	}

	err := stream.Send(start)
	if err != nil {
		result.Err = fmt.Errorf(
			"error starting stage %v: %w", id.StageIndex, err,
		)
		result.Requeue = true
		return result
	}

	// Get confirmation of the job start
	_, err = stream.Recv()
	// If this error is not
	if err != nil {
		result.Err = fmt.Errorf("error confirming stage start: %w", err)
		// If this error is the result of our maximum retry limit being reached, or the
		// job being cancelled, we do not want to restart it.
		if !errors.Is(err, ErrMaxRetriesExceeded) && !errors.Is(err, ErrJobCancelled) {
			result.Requeue = true
		}
	}

	progress, progressErr := runner.launchProgressUpdater(stream)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stageResultChan := make(chan StageResult)
	go func() {
		defer close(progress)
		defer close(stageResultChan)
		stageResultChan <- worker.RunStage(ctx, stage, progress)
	}()

	var ok bool
	select {
	case result, ok = <-stageResultChan:
		if !ok {
			panic("stage result channel closed before send")
		}
	case err = <-progressErr:
		result.Err = fmt.Errorf("error sending progress update: %w", err)
		result.Requeue = true
		return result
	}

	return result
}

func (runner *JobRunner) launchProgressUpdater(
	stream Lucy_RunnerClient,
) (progress chan<- float32, err <-chan error) {
	progressChan := make(chan float32, 32)
	errChan := make(chan error)

	go func() {
		var sendErr error
		for progressVal := range progressChan {
			if sendErr != nil {
				continue
			}

			update := &RunnerUpdate{
				Update: &RunnerUpdate_Progress{Progress: &ProgressStageUpdate{
					Progress: progressVal,
				}},
				Confirm: false,
			}

			sendErr = stream.Send(update)
			if sendErr != nil {
				errChan <- sendErr
			}
		}
	}()

	return progressChan, errChan
}
