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
) (*lucy.CreatedBatch, error) {
	batchId, err := lucydb.NewRecordId()
	if err != nil {
		return nil, fmt.Errorf("error creating record: %w", err)
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
		return nil, fmt.Errorf("error inserting document: %w", err)
	}

	// If we don't need to create any jobs, return.
	if batch.Jobs == nil || len(batch.Jobs) == 0 {
		return created, nil
	}

	// Otherwise create the jobs.
	createdJobs, err := service.CreateJobs(ctx, &lucy.NewJobs{
		Batch: batchId,
		Jobs:  batch.Jobs,
	})

	// If there is an error, return.
	if err != nil {
		return nil, fmt.Errorf("error adding jobs: %w", err)
	}
	created.JobIds = createdJobs.Ids

	return created, nil
}

// batchProjection projects all fields but the jobs field so we aren't fetching a ton
// of jobs info we don't need every time we fetch the batch.
var batchProjection = lucydb.MustCompileStaticDocument(bson.M{
	"_id":             0,
	"id":              1,
	"created":         1,
	"modified":        1,
	"type":            1,
	"name":            1,
	"description":     1,
	"job_count":       1,
	"progress":        1,
	"pending_count":   1,
	"cancelled_count": 1,
	"running_count":   1,
	"completed_count": 1,
	"success_count":   1,
	"failure_count":   1,
	"jobs.id":         1,
})

// documentDecoder is an interface for mongo results that can decode a document, like
// mongo.SingleResult and mongo.Cursor.
type documentDecoder interface {
	// Decode is the method that decodes the document.
	Decode(val interface{}) error
}

// decodeBatch handles decoding a lucy.Batch from a documentDecoder.
func decodeBatch(decoder documentDecoder, batch *lucy.Batch) error {
	// Create a value for us to decode the returned information.
	decoded := struct {
		// We'll extract the basic job info here.
		Batch *lucy.Batch `bson:",inline"`
		// Then make a struct type to receive the list of jobs which contain only an
		// 'id' field.
		JobIds []struct {
			Id *cerealMessages.UUID `bson:"id"`
		} `bson:"jobs"`
	}{}
	decoded.Batch = batch
	err := decoder.Decode(&decoded)
	if err != nil {
		return fmt.Errorf("error umarshalling batch: %w", err)
	}

	batch.Jobs = make([]*cerealMessages.UUID, len(decoded.JobIds))
	for i, jobId := range decoded.JobIds {
		batch.Jobs[i] = jobId.Id
	}

	return nil
}

// GetBatch implements lucy.LucyServer.
func (service Lucy) GetBatch(
	ctx context.Context, uuid *cerealMessages.UUID,
) (batch *lucy.Batch, err error) {
	filter := bson.M{
		"id": uuid,
	}

	opts := options.FindOne().SetProjection(batchProjection)
	result := service.db.Jobs.FindOne(ctx, filter, opts)
	if err = service.CheckMongoErr(result.Err(), ""); err != nil {
		return nil, err
	}

	batch = new(lucy.Batch)
	err = decodeBatch(result, batch)
	if err != nil {
		return nil, err
	}

	return batch, nil
}

func (service Lucy) GetBatchJobs(
	ctx context.Context, uuid *cerealMessages.UUID,
) (*lucy.BatchJobs, error) {
	filter := bson.M{"id": uuid}
	projection := bson.M{"jobs": 1}

	opts := options.FindOne().SetProjection(projection)
	result := service.db.Jobs.FindOne(ctx, filter, opts)
	if err := service.CheckMongoErr(result.Err(), ""); err != nil {
		return nil, err
	}

	jobs := new(lucy.BatchJobs)
	err := result.Decode(jobs)
	if err != nil {
		return nil, fmt.Errorf("error decoding records: %w", err)
	}

	return jobs, nil
}

// ListBatches implements lucy.LucyServer.
func (service Lucy) ListBatches(
	_ *empty.Empty, server lucy.Lucy_ListBatchesServer,
) error {
	opts := options.Find().
		// Use our projection to only return job ids
		SetProjection(batchProjection).
		// Sort newest first.
		SetSort(m{"created": -1})

	// Set up our cursor
	cursor, err := service.db.Jobs.Find(server.Context(), bson.M{}, opts)
	if err != nil {
		return fmt.Errorf("error getting db cursor: %w", err)
	}

	// Iterate over our cursor.
	batch := new(lucy.Batch)
	for cursor.Next(server.Context()) {
		// Decode the batch.
		if err = decodeBatch(cursor, batch); err != nil {
			return err
		}

		// Send the batch.
		if err = server.Send(batch); err != nil {
			return fmt.Errorf("error sending batch: %w", err)
		}
	}

	if cursor.Err() != nil {
		return fmt.Errorf("error advancing cursor: %w", err)
	}

	return nil
}

// CreateJobs implements lucy.LucyServer.
func (service Lucy) CreateJobs(
	ctx context.Context, jobs *lucy.NewJobs,
) (*lucy.CreatedJobs, error) {

	// CurrentUpdate the batch.
	filter := bson.M{
		"id": jobs.Batch,
	}

	// We need to keep track of the job id's we make so we return them in the same
	// order that the jobs were sent.
	jobIds := make([]*cerealMessages.UUID, len(jobs.Jobs))

	// We're going to use this type to make our new job records.
	type InsertJob struct {
		*lucy.NewJob `bson:",inline"`
		Id           *cerealMessages.UUID `bson:"id"`
		Created      string               `bson:"created"`
		Modified     string               `bson:"modified"`
		Status       lucy.Status          `bson:"status"`
		Progress     float32              `bson:"progress"`
		Result       lucy.Result          `bson:"result"`
		RunCount     uint32               `bson:"run_count"`
	}

	// Store our list of records to be inserted into the Batch here.
	records := make([]InsertJob, len(jobs.Jobs))
	for i, job := range jobs.Jobs {
		recordId, err := lucydb.NewRecordId()
		if err != nil {
			return nil, fmt.Errorf("error creating record: %w", err)
		}

		jobIds[i] = recordId
		records[i] = InsertJob{
			NewJob:   job,
			Id:       recordId,
			Created:  "$$NOW",
			Modified: "$$NOW",
			Status:   lucy.Status_PENDING,
			Progress: 0.0,
			Result:   lucy.Result_NONE,
			RunCount: 0,
		}
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
	err := service.CheckMongoErr(result.Err(), "batch id not found")
	if err != nil {
		return nil, err
	}

	created := &lucy.CreatedJobs{Ids: jobIds}
	return created, nil
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
