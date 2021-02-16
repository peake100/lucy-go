package service

/*
This file contains all lucy rpc methods for cancelling jobs / batches.
*/

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// cancelBatchMongoPipeline is the UpdateMany pipeline to cancel a job.
var cancelBatchMongoPipeline = db.MustCompileStaticPipeline(
	db.CancelJobsPipeline(nil),
)

var cancelBatchProjection = db.MustCompileStaticDocument(m{
	// Top-level fields.
	"id":              1,
	"modified":        1,
	"progress":        1,
	"run_count":       1,
	"job_count":       1,
	"pending_count":   1,
	"cancelled_count": 1,
	"running_count":   1,
	"completed_count": 1,
	"success_count":   1,
	"failure_count":   1,
	// Jobs fields
	"jobs.id":     1,
	"jobs.status": 1,
})

func (service Lucy) CancelBatches(
	ctx context.Context, batches *lucy.CancelBatches,
) (*empty.Empty, error) {
	filter := m{"id": m{"$in": batches.BatchIds}}
	update := cancelBatchMongoPipeline

	result, err := service.db.Jobs.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, fmt.Errorf("error running cancellation pipeline: %w", err)
	}

	batchesRequested := int64(len(batches.BatchIds))
	if result.MatchedCount != batchesRequested {
		return nil, service.errs.NewErr(
			pkerr.ErrNotFound,
			fmt.Sprintf(
				"%v batches found and cancelled, %v batches could not be found",
				result.MatchedCount,
				batchesRequested,
			),
			nil,
			nil,
		)
	}

	logger := pkmiddleware.LoggerFromCtx(ctx)
	service.cancelBatchFireEvents(batches.BatchIds, logger)

	return emptyResponse, nil
}

func (service Lucy) cancelBatchFireEvents(
	batchIds []*cerealMessages.UUID, logger zerolog.Logger,
) {
	// Launch a routine to create cancellation events for each batch.
	for _, thisBatchId := range batchIds {
		go service.cancelBatchFireBatchUpdateEvent(thisBatchId, logger)
	}
}

var cancelJobProjection = db.MustCompileStaticDocument(m{
	"id":              1,
	"modified":        1,
	"progress":        1,
	"run_count":       1,
	"job_count":       1,
	"pending_count":   1,
	"cancelled_count": 1,
	"running_count":   1,
	"completed_count": 1,
	"success_count":   1,
	"failure_count":   1,
})

func (service Lucy) CancelJob(
	ctx context.Context, job *cerealMessages.UUID,
) (*empty.Empty, error) {
	filter := m{"jobs.id": job}
	update := db.CancelJobsPipeline(job)

	opts := options.FindOneAndUpdate().
		SetProjection(cancelJobProjection).
		SetReturnDocument(options.After)

	result := service.db.Jobs.FindOneAndUpdate(ctx, filter, update, opts)
	if err := service.CheckMongoErr(result.Err(), ""); err != nil {
		return nil, err
	}

	logger := pkmiddleware.LoggerFromCtx(ctx)
	go service.cancelJobFireEvents(job, result, logger)

	return new(emptypb.Empty), nil
}

func (service Lucy) cancelJobFireEvents(
	jobId *cerealMessages.UUID,
	batchRecordDecoder documentDecoder,
	logger zerolog.Logger,
) {
	var err error
	defer func() {
		if err == nil {
			return
		}

		logger.Error().Err(err).Msg("error building batch cancellation event")
	}()

	batchData := batchInfoUpdatedResult{}
	err = batchRecordDecoder.Decode(&batchData)
	if err != nil {
		err = fmt.Errorf("error decoding batch record: %w", err)
		return
	}

	// TODO: we need to extract the job.modified field here instead of the batch
	//   modified.
	service.cancelFireJobEvents(
		batchData.BatchId, []*cerealMessages.UUID{jobId}, batchData.Modified, logger,
	)

	// Build the event.
	event := &events.BatchUpdated{
		Id:             batchData.BatchId,
		JobCount:       batchData.JobCount,
		Progress:       batchData.Progress,
		PendingCount:   batchData.PendingCount,
		CancelledCount: batchData.CancelledCount,
		RunningCount:   batchData.RunningCount,
		CompletedCount: batchData.CompletedCount,
		SuccessCount:   batchData.SuccessCount,
		FailureCount:   batchData.FailureCount,
		Modified:       batchData.Modified,
	}
	service.messenger.QueueBatchUpdated(event, logger)
}

func (service Lucy) cancelFireJobEvents(
	batchId *cerealMessages.UUID,
	jobIds []*cerealMessages.UUID,
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
	batchId *cerealMessages.UUID, logger zerolog.Logger,
) {
	var err error
	defer func() {
		if err == nil {
			return
		}
		logger.Error().Err(err).Msg("error building batch cancellation event")
	}()

	opts := options.FindOne().SetProjection(cancelBatchProjection)
	result := service.db.Jobs.FindOne(
		context.Background(), m{"id": batchId}, opts,
	)
	if result.Err() != nil {
		err = fmt.Errorf(
			"error fetching batch document: %w", result.Err(),
		)
		return
	}

	resultData := struct {
		Batch batchInfoUpdatedResult `bson:",inline"`
		Jobs  []struct {
			Id     *cerealMessages.UUID `bson:"id"`
			Status lucy.Status          `bson:"status"`
		} `bson:"jobs"`
	}{}

	err = result.Decode(&resultData)
	if err != nil {
		err = fmt.Errorf("error decoding batch record: %w", err)
		return
	}

	// Start a routine to fire the job cancelled events
	go func() {
		jobIds := make([]*cerealMessages.UUID, 0, len(resultData.Jobs))
		for _, thisJob := range resultData.Jobs {
			if thisJob.Status != lucy.Status_CANCELLED {
				continue
			}
			jobIds = append(jobIds, thisJob.Id)
		}
		service.cancelFireJobEvents(batchId, jobIds, resultData.Batch.Modified, logger)
	}()

	// Build the batch update event.
	event := &events.BatchUpdated{
		Id:             resultData.Batch.BatchId,
		JobCount:       resultData.Batch.JobCount,
		Progress:       resultData.Batch.Progress,
		PendingCount:   resultData.Batch.PendingCount,
		CancelledCount: resultData.Batch.CancelledCount,
		RunningCount:   resultData.Batch.RunningCount,
		CompletedCount: resultData.Batch.CompletedCount,
		SuccessCount:   resultData.Batch.SuccessCount,
		FailureCount:   resultData.Batch.FailureCount,
		Modified:       resultData.Batch.Modified,
	}
	service.messenger.QueueBatchUpdated(event, logger)
}
