package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/lucy"
	"github.com/peake100/lucy-go/service/lucydb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// Make these simple aliases to make our updates a little more readable.
type m = bson.M
type arr = bson.A

// CreateBatch implements lucy.LucyServer.
func (service Lucy) CreateBatch(
	ctx context.Context, batch *lucy.NewBatch,
) (*cerealMessages.UUID, error) {
	recordId, err := lucydb.NewRecordId()
	if err != nil {
		return nil, fmt.Errorf("error creating record: %w", err)
	}

	currentTime := time.Now().UTC()

	batchRecord := &struct {
		*lucy.NewBatch `bson:",inline"`
		Id             *cerealMessages.UUID `bson:"id"`
		Created        time.Time            `bson:"created"`
		Modified       time.Time            `bson:"modified"`
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
		NewBatch:       batch,
		Id:             recordId,
		Created:        currentTime,
		Modified:       currentTime,
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

	_, err = service.db.Jobs.InsertOne(ctx, batchRecord)
	if err != nil {
		return nil, fmt.Errorf("error inserting document: %w", err)
	}

	return recordId, nil
}

// GetBatch implements lucy.LucyServer.
func (service Lucy) GetBatch(
	ctx context.Context, uuid *cerealMessages.UUID,
) (batch *lucy.Batch, err error) {
	filter := bson.M{
		"id": uuid,
	}

	opts := options.FindOne().SetProjection(bson.M{"_id": 0, "jobs": 0})
	result := service.db.Jobs.FindOne(ctx, filter, opts)
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return nil, pkerr.ErrNotFound
		}
		return nil, fmt.Errorf("error getting result")
	}

	batch = new(lucy.Batch)
	err = result.Decode(batch)
	if err != nil {
		return nil, fmt.Errorf("error umarshalling batch: %w", err)
	}

	return batch, nil
}

// ListBatches implements lucy.LucyServer.
func (service Lucy) ListBatches(
	_ *empty.Empty, server lucy.Lucy_ListBatchesServer,
) error {
	panic("implement me")
}

// CreateJobs implements lucy.LucyServer.
func (service Lucy) CreateJobs(
	ctx context.Context, jobs *lucy.NewJobs,
) (*lucy.CreatedJobs, error) {

	// We're going to use this type to make our new job records.
	type InsertJob struct {
		*lucy.NewJob `bson:",inline"`
		Id          *cerealMessages.UUID `bson:"id"`
		Created     string `bson:"created"`
		Modified    string `bson:"modified"`
		Status      lucy.Status `bson:"status"`
		Progress    float32 `bson:"progress"`
		Result      lucy.Result `bson:"result"`
		RunCount    uint32 `bson:"run_count"`
	}

	// We need to keep track of the job id's we make so we return them in the same
	// order that the jobs were sent.
	jobIds := make([]*cerealMessages.UUID, len(jobs.Jobs))

	// Store our list of records to be inserted into the Batch here.
	records := make([]InsertJob, len(jobs.Jobs))

	for i, job := range jobs.Jobs {
		recordId, err := lucydb.NewRecordId()
		if err != nil {
			return nil, fmt.Errorf("error creating record: %w", err)
		}

		jobIds[i] = recordId

		records[i] = InsertJob{
			NewJob:      job,
			Id:          recordId,
			Created:     "$$NOW",
			Modified:    "$$NOW",
			Status:      lucy.Status_PENDING,
			Progress:    0.0,
			Result:      lucy.Result_NONE,
			RunCount:    0,
		}
	}

	// Update the batch.
	filter := bson.M{
		"id": jobs.Batch,
	}

	updatePipeline := arr{
		// Add the job to the batch.
		m{
			"$set": m{
				// The batch may already have jobs, so we need to make sure we are
				// appending to the end of the curent list.
				"jobs": m{
					"$concatArrays": bson.A{"$jobs", records},
				},
				"modified": "$$NOW",
			},
		},
		lucydb.UpdateBatchSummaries,
		lucydb.FinalizeBatchProgressStage,
	}

	// Return an emtpy record.
	opts := new(options.FindOneAndUpdateOptions).SetProjection(bson.M{"_id": 1})
	result := service.db.Jobs.FindOneAndUpdate(ctx, filter, updatePipeline, opts)
	if result.Err() == nil {
		created := &lucy.CreatedJobs{Ids: jobIds}
		return created, nil
	}

	if errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return nil, service.errs.NewErr(
			pkerr.ErrNotFound, "batch id not found", nil, result.Err(),
		)
	}

	return nil, result.Err()
}

// GetJob implements lucy.LucyServer.
func (service Lucy) GetJob(
	ctx context.Context, uuid *cerealMessages.UUID,
) (*lucy.Job, error) {
	filter := bson.M{"jobs.id": uuid}
	projection := bson.M{"jobs.$": 1}

	opts := options.FindOne().SetProjection(projection)
	result := service.db.Jobs.FindOne(ctx, filter, opts)
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return nil, pkerr.ErrNotFound
		}
		return nil, fmt.Errorf("error getting job: %w", result.Err())
	}

	job := &struct {
		Jobs []*lucy.Job `bson:"jobs"`
	}{}
	err := result.Decode(job)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling job: %w", err)
	}

	return job.Jobs[0], nil
}

// ListJobs implements lucy.LucyServer.
func (service Lucy) ListJobs(
	_ *empty.Empty, server lucy.Lucy_ListJobsServer,
) error {
	panic("implement me")
}
