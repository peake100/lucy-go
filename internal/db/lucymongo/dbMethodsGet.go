package lucymongo

import (
	"context"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// GetBatch implements db.Connection.
func (backend Backend) GetBatch(
	ctx context.Context, BatchId *cerealMessages.UUID,
) (db.ResultGetBatch, error) {
	filter := bson.M{"id": BatchId}

	opts := options.FindOne().SetProjection(batchProjection)
	mongoResult := backend.Jobs.FindOne(ctx, filter, opts)
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

// GetJob implements db.Connection.
func (backend Backend) GetJob(
	ctx context.Context, JobId *cerealMessages.UUID,
) (db.ResultGetJob, error) {
	filter := bson.M{"jobs.id": JobId}
	projection := bson.M{"jobs.$": 1}

	opts := options.FindOne().SetProjection(projection)
	mongoResult := backend.Jobs.FindOne(ctx, filter, opts)
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

func (backend Backend) GetBatchJobs(
	ctx context.Context, BatchId *cerealMessages.UUID,
) (db.ResultGetBatchJobs, error) {
	filter := bson.M{"id": BatchId}
	projection := bson.M{"jobs": 1}

	opts := options.FindOne().SetProjection(projection)
	mongoResult := backend.Jobs.FindOne(ctx, filter, opts)
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
