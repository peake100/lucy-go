package service

/*
This file contains all lucy rpc methods for fetching jobs / batches.
*/

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/lucy-go/internal/db/lucymongo"
	"github.com/peake100/lucy-go/pkg/lucy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// GetBatch implements lucy.LucyServer.
func (service Lucy) GetBatch(
	ctx context.Context, uuid *cerealMessages.UUID,
) (batch *lucy.Batch, err error) {
	result, err := service.db.GetBatch(ctx, uuid)
	return result.Batch, nil
}

// GetJob implements lucy.LucyServer.
func (service Lucy) GetJob(
	ctx context.Context, uuid *cerealMessages.UUID,
) (*lucy.Job, error) {
	result, err := service.db.GetJob(ctx, uuid)
	return result.Job, err
}

// GetBatchJobs implements lucy.LucyServer.
func (service Lucy) GetBatchJobs(
	ctx context.Context, uuid *cerealMessages.UUID,
) (*lucy.BatchJobs, error) {
	result, err := service.db.GetBatchJobs(ctx, uuid)
	return result.Jobs, err
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
	cursor, err := service.dbMongo.Jobs.Find(server.Context(), bson.M{}, opts)
	if err != nil {
		return fmt.Errorf("error getting dbMongo cursor: %w", err)
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
var batchProjection = lucymongo.MustCompileStaticDocument(bson.M{
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
