package service

/*
This file contains all lucy rpc methods for making updates to a job stage during a job
run.
*/

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"
)

// emptyResponse will be used to respond to requests where we can use an empty response
// so we don't have to make it more than once.
var emptyResponse = new(emptypb.Empty)

// fireUpdatedEventsFunc is the function signature for firing events after an update
// has been made.
type fireUpdatedEventsFunc = func(
	ctx context.Context, dbResult db.ResultWorkerUpdate, logger zerolog.Logger,
) error

func (service Lucy) updateJobStageFireEvents(
	handlerCtx context.Context,
	update db.ResultWorkerUpdate,
	eventsFunc fireUpdatedEventsFunc,
	updateBatch bool,
) {
	// Log any errors on the way out.
	logger := pkmiddleware.LoggerFromCtx(handlerCtx)
	var err error
	defer func() {
		if err == nil {
			return
		}
		logger.Error().Err(err).Msg("error firing events.")
	}()

	// TODO: use the resources ctx (and WaitGroup) here.
	err = eventsFunc(context.Background(), update, logger)
	if err != nil {
		err = fmt.Errorf("error firing events")
	}

	// Fire a batch info update
	if updateBatch {
		service.updateJobStageFireBatchUpdate(handlerCtx, update, logger)
	}
}

func (service Lucy) updateJobStageFireBatchUpdate(
	ctx context.Context, updated db.ResultWorkerUpdate, logger zerolog.Logger,
) {
	event := &events.BatchUpdated{
		Id:             updated.Batch.BatchId,
		JobCount:       updated.Batch.JobCount,
		Progress:       updated.Batch.Progress,
		PendingCount:   updated.Batch.PendingCount,
		CancelledCount: updated.Batch.CancelledCount,
		RunningCount:   updated.Batch.RunningCount,
		CompletedCount: updated.Batch.CompletedCount,
		SuccessCount:   updated.Batch.SuccessCount,
		FailureCount:   updated.Batch.FailureCount,
	}

	service.messenger.QueueBatchUpdated(event, logger)
}

var validStartStatuses = []lucy.Status{
	lucy.Status_PENDING, lucy.Status_RUNNING, lucy.Status_COMPLETED,
}

func (service Lucy) StartStage(
	ctx context.Context, req *lucy.StartStage,
) (*empty.Empty, error) {
	update := db.StageUpdate{
		StageId:  req.StageId,
		TypeName: db.UpdateTypeStart,

		ValidateRunCount: true,
		ValidStatuses:    validStartStatuses,

		IncrementRunCount: true,
		ModifiedTimeField: "started",

		RunBy: db.StageUpdateRunBy{
			Value:  req.Update.RunBy,
			Update: true,
		},
		Status: lucy.Status_RUNNING,
		Progress: db.StageUpdateProgress{
			Value:  0,
			Update: true,
		},
		Result: db.StageUpdateResult{
			Value:  lucy.Result_NONE,
			Update: true,
		},
		ResultData: db.StageUpdateResultData{
			Value:  nil,
			Update: true,
		},
		Error: db.StageUpdateError{
			Value:  nil,
			Update: true,
		},
	}

	dbResult, err := service.db.UpdateStage(ctx, update)
	if err != nil {
		return nil, err
	}

	go service.updateJobStageFireEvents(
		ctx,
		dbResult,
		service.startStageFireEvents,
		// We only need to update the batch if we are starting the first stage.
		req.StageId.StageIndex == 0,
	)
	return emptyResponse, nil
}

func (service Lucy) startStageFireEvents(
	ctx context.Context, dbResult db.ResultWorkerUpdate, logger zerolog.Logger,
) error {
	stageEvent := &events.StageStart{
		Id: &events.StageId{
			BatchId:    dbResult.Batch.BatchId,
			JobId:      dbResult.StageId.JobId,
			StageIndex: dbResult.StageId.StageIndex,
		},
		RunBy:    dbResult.Stage.RunBy,
		RunCount: dbResult.Stage.RunCount,
		Modified: dbResult.Batch.Modified,
	}
	service.messenger.QueueStageStart(stageEvent, logger)

	// If this is not the first stage being started, exit.
	if dbResult.StageId.StageIndex != 0 {
		return nil
	}

	// Fire the Job Start event.
	jobEvent := &events.JobStart{
		Id: &events.JobId{
			BatchId: dbResult.Batch.BatchId,
			JobId:   dbResult.StageId.JobId,
		},
		RunBy:    dbResult.Stage.RunBy,
		RunCount: dbResult.Job.RunCount,
		Modified: dbResult.Batch.Modified,
	}
	service.messenger.QueueJobStart(jobEvent, logger)
	return nil
}

var validProgressStatuses = []lucy.Status{lucy.Status_RUNNING}

func (service Lucy) ProgressStage(
	ctx context.Context, req *lucy.ProgressStage,
) (*empty.Empty, error) {
	update := db.StageUpdate{
		StageId:  req.StageId,
		TypeName: db.UpdateTypeProgress,

		ValidateRunCount: false,
		ValidStatuses:    validProgressStatuses,

		IncrementRunCount: false,
		ModifiedTimeField: "",

		RunBy: db.StageUpdateRunBy{
			Value:  "",
			Update: false,
		},
		Status: lucy.Status_RUNNING,
		Progress: db.StageUpdateProgress{
			Value:  req.Update.Progress,
			Update: true,
		},
		Result: db.StageUpdateResult{
			Value:  0,
			Update: false,
		},
		ResultData: db.StageUpdateResultData{
			Value:  nil,
			Update: false,
		},
		Error: db.StageUpdateError{
			Value:  nil,
			Update: false,
		},
	}

	dbResult, err := service.db.UpdateStage(ctx, update)
	if err != nil {
		return nil, err
	}

	service.updateJobStageFireEvents(
		ctx, dbResult, service.progressStageFireEvents, true,
	)
	return emptyResponse, nil
}

func (service Lucy) progressStageFireEvents(
	ctx context.Context, dbResult db.ResultWorkerUpdate, logger zerolog.Logger,
) error {
	// Fire off the stage progress event.
	stageEvent := &events.StageProgress{
		Id: &events.StageId{
			BatchId:    dbResult.Batch.BatchId,
			JobId:      dbResult.StageId.JobId,
			StageIndex: dbResult.StageId.StageIndex,
		},
		Progress: dbResult.Stage.Progress,
		Modified: dbResult.Batch.Modified,
	}
	service.messenger.QueueStageProgress(stageEvent, logger)

	// Fire off the job progress event.
	jobEvent := &events.JobProgress{
		Id: &events.JobId{
			BatchId: dbResult.Batch.BatchId,
			JobId:   dbResult.Job.Id,
		},
		Progress: dbResult.Job.Progress,
		Modified: dbResult.Batch.Modified,
	}
	service.messenger.QueueJobProgress(jobEvent, logger)

	return nil
}

var validCompleteStatuses = []lucy.Status{lucy.Status_RUNNING}

func (service Lucy) CompleteStage(
	ctx context.Context, req *lucy.CompleteStage,
) (*empty.Empty, error) {
	result := lucy.Result_SUCCEEDED

	// progress is only going to be updated on a success, otherwise it will stay where
	// it is.
	var progress float32
	var updateProgress bool
	if req.Update.Error != nil {
		result = lucy.Result_FAILED
	} else {
		// If there was no error, progress is set to 1.0, it is fully complete.
		progress = float32(1.0)
		updateProgress = true
	}

	update := db.StageUpdate{
		StageId:  req.StageId,
		TypeName: db.UpdateTypeComplete,

		ValidateRunCount: false,
		ValidStatuses:    validCompleteStatuses,

		IncrementRunCount: false,
		ModifiedTimeField: "completed",

		RunBy: db.StageUpdateRunBy{
			Value:  "",
			Update: false,
		},
		Status: lucy.Status_COMPLETED,
		Progress: db.StageUpdateProgress{
			Value:  progress,
			Update: updateProgress,
		},
		Result: db.StageUpdateResult{
			Value:  result,
			Update: true,
		},
		ResultData: db.StageUpdateResultData{
			Value:  req.Update.ResultData,
			Update: true,
		},
		Error: db.StageUpdateError{
			Value:  req.Update.Error,
			Update: true,
		},
	}

	dbResult, err := service.db.UpdateStage(ctx, update)
	if err != nil {
		return nil, err
	}

	service.updateJobStageFireEvents(
		ctx, dbResult, service.completeStageFireEvents, true,
	)
	return emptyResponse, nil
}

func (service Lucy) completeStageFireEvents(
	ctx context.Context, dbResult db.ResultWorkerUpdate, logger zerolog.Logger,
) error {
	// Fire stage complete event.
	stageEvent := &events.StageComplete{
		Id: &events.StageId{
			BatchId:    dbResult.Batch.BatchId,
			JobId:      dbResult.StageId.JobId,
			StageIndex: dbResult.StageId.StageIndex,
		},
		Result:   dbResult.Stage.Result,
		Modified: dbResult.Batch.Modified,
	}
	service.messenger.QueueStageComplete(stageEvent, logger)

	// TODO: decide whether we should fire a progress = 1.0 update here or just let
	//   client applications infer that from the completion event.

	// If the job had a result, return it.
	if dbResult.Job.Result != lucy.Result_NONE {
		// Otherwise, fire off a job completed event.
		jobEvent := &events.JobComplete{
			Id: &events.JobId{
				BatchId: dbResult.Batch.BatchId,
				JobId:   dbResult.StageId.JobId,
			},
			Result:   dbResult.Job.Result,
			Modified: dbResult.Batch.Modified,
		}
		service.messenger.QueueJobComplete(jobEvent, logger)
	}

	return nil
}
