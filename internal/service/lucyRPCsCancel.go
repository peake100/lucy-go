package service

/*
This file contains all lucy rpc methods for cancelling jobs / batches.
*/

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service Lucy) CancelBatches(
	ctx context.Context, batches *lucy.CancelBatches,
) (*empty.Empty, error) {
	cursor, err := service.db.CancelBatches(ctx, batches)
	if err != nil {
		return nil, err
	}

	logger := pkmiddleware.LoggerFromCtx(ctx)
	service.cancelBatchFireEvents(ctx, cursor, logger)

	return emptyResponse, nil
}

func (service Lucy) cancelBatchFireEvents(
	ctx context.Context, cursor db.CancelBatchResultsCursor, logger zerolog.Logger,
) {
	// Launch a routine to create cancellation events for each batch.
	for {
		batchInfo, err := cursor.Next(ctx)
		if err != nil {
			logger.Error().
				Err(fmt.Errorf(
					"error fetching batch info from dbMongo cursor: %w", err,
				)).
				Msg("error sending batch cancelled events")
			return
		}
		go service.cancelBatchFireBatchUpdateEvent(batchInfo, logger)
	}
}

func (service Lucy) CancelJob(
	ctx context.Context, job *cereal.UUID,
) (*empty.Empty, error) {
	result, err := service.db.CancelJob(ctx, job)
	if err != nil {
		return nil, err
	}

	logger := pkmiddleware.LoggerFromCtx(ctx)
	go service.cancelJobFireEvents(job, result.BatchSummaries, logger)

	return new(emptypb.Empty), nil
}

func (service Lucy) cancelJobFireEvents(
	jobId *cereal.UUID,
	batchInfo db.ResultBatchSummaries,
	logger zerolog.Logger,
) {
	// TODO: we need to extract the job.modified field here instead of the batch
	//   modified.
	service.cancelFireJobEvents(
		batchInfo.BatchId, []*cereal.UUID{jobId}, batchInfo.Modified, logger,
	)

	// Build the event.
	event := &events.BatchUpdated{
		Id:             batchInfo.BatchId,
		JobCount:       batchInfo.JobCount,
		Progress:       batchInfo.Progress,
		PendingCount:   batchInfo.PendingCount,
		CancelledCount: batchInfo.CancelledCount,
		RunningCount:   batchInfo.RunningCount,
		CompletedCount: batchInfo.CompletedCount,
		SuccessCount:   batchInfo.SuccessCount,
		FailureCount:   batchInfo.FailureCount,
		Modified:       batchInfo.Modified,
	}
	service.messenger.QueueBatchUpdated(event, logger)
}

func (service Lucy) cancelFireJobEvents(
	batchId *cereal.UUID,
	jobIds []*cereal.UUID,
	modified *timestamppb.Timestamp,
	logger zerolog.Logger,
) {
	for _, thisJobId := range jobIds {
		event := &events.JobCancelled{
			Id: &events.JobId{
				BatchId: batchId,
				JobId:   thisJobId,
			},
			Modified: modified,
		}

		service.messenger.QueueJobCancel(event, logger)
	}
}

func (service Lucy) cancelBatchFireBatchUpdateEvent(
	batchInfo db.ResultCancelBatch, logger zerolog.Logger,
) {
	// Start a routine to fire the job cancelled events
	go func() {
		service.cancelFireJobEvents(
			batchInfo.BatchSummaries.BatchId,
			batchInfo.JobIds,
			batchInfo.BatchSummaries.Modified,
			logger,
		)
	}()

	// Build the batch update event.
	event := &events.BatchUpdated{
		Id:             batchInfo.BatchSummaries.BatchId,
		JobCount:       batchInfo.BatchSummaries.JobCount,
		Progress:       batchInfo.BatchSummaries.Progress,
		PendingCount:   batchInfo.BatchSummaries.PendingCount,
		CancelledCount: batchInfo.BatchSummaries.CancelledCount,
		RunningCount:   batchInfo.BatchSummaries.RunningCount,
		CompletedCount: batchInfo.BatchSummaries.CompletedCount,
		SuccessCount:   batchInfo.BatchSummaries.SuccessCount,
		FailureCount:   batchInfo.BatchSummaries.FailureCount,
		Modified:       batchInfo.BatchSummaries.Modified,
	}
	service.messenger.QueueBatchUpdated(event, logger)
}
