package service

/*
This file contains all lucy rpc methods for creating jobs / batches.
*/

import (
	"context"
	"fmt"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"go.mongodb.org/mongo-driver/bson"
)

// Make these simple aliases to make our updates a little more readable.
type m = bson.M
type arr = bson.A

// CreateBatch implements lucy.LucyServer.
func (service Lucy) CreateBatch(
	ctx context.Context, batch *lucy.NewBatch,
) (*lucy.CreatedBatch, error) {
	result, err := service.db.CreateBatch(ctx, batch)
	if err != nil {
		return nil, err
	}

	event := &events.BatchCreated{
		Id:       result.BatchId,
		Modified: result.Modified,
	}

	// Fire the batch created event.
	logger := pkmiddleware.LoggerFromCtx(ctx)
	service.messenger.QueueBatchCreated(event, logger)

	created := new(lucy.CreatedBatch)
	created.BatchId = result.BatchId

	// If we don't need to create any jobs, return.
	if batch.Jobs == nil || len(batch.Jobs) == 0 {
		return created, nil
	}

	// Otherwise create the jobs.
	createdJobs, err := service.CreateJobs(ctx, &lucy.NewJobs{
		Batch: result.BatchId,
		Jobs:  batch.Jobs,
	})

	// If there is an error, return.
	if err != nil {
		return nil, fmt.Errorf("error adding jobs: %w", err)
	}
	created.JobIds = createdJobs.Ids

	return created, nil
}
