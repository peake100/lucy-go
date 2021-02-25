package lucymongo

import (
	"context"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
)

// GetBatch implements db.Backend.
func (backend Backend) GetBatch(
	ctx context.Context, BatchId *cereal.UUID,
) (db.ResultGetBatch, error) {
	filter := bson.M{"id": BatchId}

	opts := options.FindOne().SetProjection(batchProjection)
	mongoResult := backend.jobs.FindOne(ctx, filter, opts)
	if err := backend.CheckMongoErr(mongoResult.Err(), ""); err != nil {
		return db.ResultGetBatch{}, err
	}

	batch := new(lucy.Batch)
	err := decodeBatch(mongoResult, batch)
	if err != nil {
		return db.ResultGetBatch{}, err
	}

	result := db.ResultGetBatch{Batch: batch}
	return result, nil
}

// GetJob implements db.Backend.
func (backend Backend) GetJob(
	ctx context.Context, JobId *cereal.UUID,
) (db.ResultGetJob, error) {
	filter := bson.M{"jobs.id": JobId}
	projection := bson.M{"jobs.$": 1}

	opts := options.FindOne().SetProjection(projection)
	mongoResult := backend.jobs.FindOne(ctx, filter, opts)
	err := backend.CheckMongoErr(mongoResult.Err(), "")
	if err != nil {
		return db.ResultGetJob{}, err
	}

	job := &struct {
		Jobs []*lucy.Job `bson:"jobs"`
	}{}
	err = mongoResult.Decode(job)
	if err != nil {
		return db.ResultGetJob{}, fmt.Errorf("error unmarshalling job: %w", err)
	}

	result := db.ResultGetJob{Job: job.Jobs[0]}
	return result, nil
}

// batchProjection projects all fields but the jobs field so we aren't fetching a ton
// of jobs info we don't need every time we fetch the batch.
var batchProjection = MustCompileStaticDocument(bson.M{
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
})

// documentDecoder is an interface for mongo results that can decode a document, like
// mongo.SingleResult and mongo.Cursor.
type documentDecoder interface {
	// Decode is the method that decodes the document.
	Decode(val interface{}) error
}

// decodeBatch handles decoding a lucy.Batch from a documentDecoder.
func decodeBatch(decoder documentDecoder, batch *lucy.Batch) error {
	err := decoder.Decode(batch)
	if err != nil {
		return fmt.Errorf("error umarshalling batch: %w", err)
	}

	return nil
}

func (backend Backend) GetBatchJobs(
	ctx context.Context, BatchId *cereal.UUID,
) (db.ResultGetBatchJobs, error) {
	filter := bson.M{"id": BatchId}
	projection := bson.M{"jobs": 1}

	opts := options.FindOne().SetProjection(projection)
	mongoResult := backend.jobs.FindOne(ctx, filter, opts)
	if err := backend.CheckMongoErr(mongoResult.Err(), ""); err != nil {
		return db.ResultGetBatchJobs{}, err
	}

	jobs := new(lucy.BatchJobs)
	err := mongoResult.Decode(jobs)
	if err != nil {
		return db.ResultGetBatchJobs{}, fmt.Errorf(
			"error decoding records: %w", err,
		)
	}

	result := db.ResultGetBatchJobs{
		Jobs: jobs,
	}

	return result, nil
}

// listBatchesOpts is the options object we will use for listing batches
var listBatchesOpts = options.Find().
	// Use our projection to only return job ids
	SetProjection(batchProjection).
	// Sort newest first.
	SetSort(m{"created": -1})

func (backend Backend) ListBatches(ctx context.Context) (db.ListBatchesCursor, error) {
	// Set up our cursor
	cursor, err := backend.jobs.Find(ctx, bson.M{}, listBatchesOpts)
	if err != nil {
		return nil, fmt.Errorf("error getting mongo cursor: %w", err)
	}

	return listBatchesCursor{cursor: cursor}, nil
}

// listBatchesCursor implements db.ListBatchesCursor for returning a list of all batches
// to the user.
type listBatchesCursor struct {
	cursor *mongo.Cursor
}

func (cursor listBatchesCursor) Next(ctx context.Context) (*lucy.Batch, error) {
	ok := cursor.cursor.Next(ctx)
	if !ok {
		if cursor.cursor.Err() == nil {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("error advancing cursor: %w", cursor.cursor.Err())
	}

	result := new(lucy.Batch)
	err := cursor.cursor.Decode(result)
	if err != nil {
		return nil, fmt.Errorf("error decoding batch document: %w", err)
	}

	return result, nil
}
