package lucymongo

import (
	"context"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/internal/db/internal"
	"github.com/peake100/lucy-go/pkg/lucy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (backend Backend) CreateBatch(
	ctx context.Context, batch *lucy.NewBatch,
) (db.ResultCreateBatch, error) {
	batchId, err := internal.NewRecordId()
	if err != nil {
		return db.ResultCreateBatch{}, fmt.Errorf(
			"error creating record: %w", err,
		)
	}

	currentTime := time.Now().UTC()

	batchRecord := &struct {
		Id             *cereal.UUID `bson:"id"`
		Created        time.Time    `bson:"created"`
		Modified       time.Time    `bson:"modified"`
		Type           string       `bson:"type"`
		Name           string       `bson:"name"`
		Description    string       `bson:"description"`
		Jobs           []*lucy.Job  `bson:"jobs"`
		JobCount       uint32       `bson:"job_count"`
		PendingCount   uint32       `bson:"pending_count"`
		CancelledCount uint32       `bson:"cancelled_count"`
		RunningCount   uint32       `bson:"running_count"`
		CompletedCount uint32       `bson:"completed_count"`
		SuccessCount   uint32       `bson:"success_count"`
		FailureCount   uint32       `bson:"failure_count"`
		Progress       float32      `bson:"progress"`
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

	_, err = backend.jobs.InsertOne(ctx, batchRecord)
	if err != nil {
		return db.ResultCreateBatch{}, fmt.Errorf(
			"error inserting document: %w", err,
		)
	}

	result := db.ResultCreateBatch{
		BatchId:  batchId,
		Created:  timestamppb.New(currentTime),
		Modified: timestamppb.New(currentTime),
	}

	return result, nil
}

// createJobProjection is the projection we will apply to job creation.
var createJobProjection = MustCompileStaticDocument(m{
	"id":              1,
	"modified":        1,
	"progress":        1,
	"job_count":       1,
	"pending_count":   1,
	"cancelled_count": 1,
	"running_count":   1,
	"completed_count": 1,
	"success_count":   1,
	"failure_count":   1,
})

func (backend Backend) CreateJobs(
	ctx context.Context, jobs *lucy.NewJobs,
) (db.ResultCreateJobs, error) {
	// CurrentUpdate the batch.
	filter := bson.M{"id": jobs.Batch}

	// We need to keep track of the job id's we make so we return them in the same
	// jobQueueOrder that the jobs were sent.
	jobIds := make([]*cereal.UUID, len(jobs.Jobs))
	for i := range jobs.Jobs {
		recordId, err := internal.NewRecordId()
		if err != nil {
			return db.ResultCreateJobs{}, fmt.Errorf(
				"error creating record: %w", err,
			)
		}
		jobIds[i] = recordId
	}

	// Create the update pipeline for adding the jobs to the batch record.
	updatePipeline := CreateAddJobPipeline(jobs, jobIds)

	// Return an emtpy record.
	opts := new(options.FindOneAndUpdateOptions).
		SetProjection(createJobProjection).
		SetReturnDocument(options.After)

	dbResult := backend.jobs.FindOneAndUpdate(ctx, filter, updatePipeline, opts)
	err := backend.CheckMongoErr(dbResult.Err(), "batch id not found")
	if err != nil {
		return db.ResultCreateJobs{}, err
	}

	created := &lucy.CreatedJobs{Ids: jobIds}
	batchInfo := db.ResultBatchSummaries{}
	err = dbResult.Decode(&batchInfo)
	if err != nil {
		return db.ResultCreateJobs{}, fmt.Errorf(
			"error decoding return document: %w", err,
		)
	}

	result := db.ResultCreateJobs{
		Jobs:           created,
		BatchSummaries: batchInfo,
	}

	return result, nil
}
