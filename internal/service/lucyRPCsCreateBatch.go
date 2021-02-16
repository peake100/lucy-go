package service

/*
This file contains all lucy rpc methods for creating jobs / batches.
*/

import (
	"context"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// Make these simple aliases to make our updates a little more readable.
type m = bson.M
type arr = bson.A

// CreateBatch implements lucy.LucyServer.
func (service Lucy) CreateBatch(
	ctx context.Context, batch *lucy.NewBatch,
) (*lucy.CreatedBatch, error) {
	created, modified, err := service.createBatchInsertDBRecord(ctx, batch)
	if err != nil {
		return nil, err
	}

	event := &events.BatchCreated{
		Id:       created.BatchId,
		Modified: modified,
	}

	// Fire the batch created event.
	logger := pkmiddleware.LoggerFromCtx(ctx)
	service.messenger.QueueBatchCreated(event, logger)

	// If we don't need to create any jobs, return.
	if batch.Jobs == nil || len(batch.Jobs) == 0 {
		return created, nil
	}

	// Otherwise create the jobs.
	createdJobs, err := service.CreateJobs(ctx, &lucy.NewJobs{
		Batch: created.BatchId,
		Jobs:  batch.Jobs,
	})

	// If there is an error, return.
	if err != nil {
		return nil, fmt.Errorf("error adding jobs: %w", err)
	}
	created.JobIds = createdJobs.Ids

	return created, nil
}

func (service Lucy) createBatchInsertDBRecord(
	ctx context.Context, batch *lucy.NewBatch,
) (*lucy.CreatedBatch, *timestamppb.Timestamp, error) {
	batchId, err := db.NewRecordId()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating record: %w", err)
	}

	currentTime := time.Now().UTC()

	batchRecord := &struct {
		Id             *cerealMessages.UUID `bson:"id"`
		Created        time.Time            `bson:"created"`
		Modified       time.Time            `bson:"modified"`
		Type           string               `bson:"type"`
		Name           string               `bson:"name"`
		Description    string               `bson:"description"`
		Jobs           []*lucy.Job          `bson:"jobs"`
		JobCount       uint32               `bson:"job_count"`
		PendingCount   uint32               `bson:"pending_count"`
		CancelledCount uint32               `bson:"cancelled_count"`
		RunningCount   uint32               `bson:"running_count"`
		CompletedCount uint32               `bson:"completed_count"`
		SuccessCount   uint32               `bson:"success_count"`
		FailureCount   uint32               `bson:"failure_count"`
		Progress       float32              `bson:"progress"`
	}{
		Id:             batchId,
		Created:        currentTime,
		Modified:       currentTime,
		Type:           batch.Type,
		Name:           batch.Name,
		Description:    batch.Description,
		Jobs:           make([]*lucy.Job, 0),
		JobCount:       0,
		PendingCount:   0,
		CancelledCount: 0,
		RunningCount:   0,
		CompletedCount: 0,
		SuccessCount:   0,
		FailureCount:   0,
		Progress:       0,
	}

	created := &lucy.CreatedBatch{
		BatchId: batchId,
		JobIds:  nil,
	}

	_, err = service.db.Jobs.InsertOne(ctx, batchRecord)
	if err != nil {
		return nil, nil, fmt.Errorf("error inserting document: %w", err)
	}

	return created, timestamppb.New(currentTime), nil
}
