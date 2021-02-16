package service

/*
This file contains all lucy rpc methods for fetching jobs / batches.
*/

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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

// batchProjection projects all fields but the jobs field so we aren't fetching a ton
// of jobs info we don't need every time we fetch the batch.
var batchProjection = db.MustCompileStaticDocument(bson.M{
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
