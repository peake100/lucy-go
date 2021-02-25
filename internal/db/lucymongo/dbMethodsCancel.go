package lucymongo

import (
	"context"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
)

func (backend Backend) CancelBatches(
	ctx context.Context, batches *lucy.CancelBatches,
) (db.CancelBatchResultsCursor, error) {
	// Do the cancellation update across many batch ids.
	filter := m{"id": m{"$in": batches.BatchIds}}
	update := cancelBatchMongoPipeline

	updateResult, err := backend.jobs.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, fmt.Errorf(
			"error running cancellation pipeline: %w", err,
		)
	}

	batchesRequested := int64(len(batches.BatchIds))
	if updateResult.MatchedCount != batchesRequested {
		return nil, backend.errsGen.NewErr(
			pkerr.ErrNotFound,
			fmt.Sprintf(
				"%v batches found and cancelled, %v batches could not be found",
				updateResult.MatchedCount,
				batchesRequested,
			),
			nil,
			nil,
		)
	}

	// Get the updated batch summaries.
	cursor, err := backend.jobs.Find(
		ctx, filter, options.Find().SetProjection(cancelBatchProjection),
	)

	if err != nil {
		return nil, fmt.Errorf("error fetching updated documents: %w", err)
	}

	return &mongoCancelBatchResultsCursor{mongo: cursor}, nil
}

// mongoCancelBatchResultsCursor implements db.CancelBatchResultsCursor.
type mongoCancelBatchResultsCursor struct {
	// mongo holds the underlying mongodb cursor.
	mongo *mongo.Cursor
}

func (cursor *mongoCancelBatchResultsCursor) Next(
	ctx context.Context,
) (result db.ResultCancelBatch, err error) {
	ok := cursor.mongo.Next(ctx)
	if !ok {
		if cursor.mongo.Err() != nil {
			return result, fmt.Errorf(
				"error fetching document from cursor: %w", cursor.mongo.Err(),
			)
		}
		return result, io.EOF
	}

	decoded := struct {
		Batch *db.ResultBatchSummaries `bson:",inline"`
		Jobs  []struct {
			Id     *cereal.UUID `bson:"id"`
			Status lucy.Status  `bson:"status"`
		}
	}{}

	// We'll make the batch info field a pointer to our *result's* batchInfo field so
	// we are decoding directly into the result.
	decoded.Batch = &result.BatchSummaries

	// Decode our result.
	err = cursor.mongo.Decode(&decoded)
	if err != nil {
		return result, fmt.Errorf("error decoding batch document: %w", err)
	}

	// Allocate a slice with enough capacity for all of the job ids on the result.
	result.JobIds = make([]*cereal.UUID, 0, len(decoded.Jobs))

	// Extract the id's of cancelled jobs.
	for _, job := range decoded.Jobs {
		if job.Status != lucy.Status_CANCELLED {
			continue
		}

		result.JobIds = append(result.JobIds, job.Id)
	}

	return result, nil
}

// cancelBatchMongoPipeline is the UpdateMany pipeline to cancel a job.
var cancelBatchMongoPipeline = MustCompileStaticPipeline(
	CancelJobsPipeline(nil),
)

var cancelBatchProjection = MustCompileStaticDocument(m{
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
	// jobs fields
	"jobs.id":     1,
	"jobs.status": 1,
})

func (backend Backend) CancelJob(
	ctx context.Context, job *cereal.UUID,
) (db.ResultCancelJob, error) {
	filter := m{"jobs.id": job}
	update := CancelJobsPipeline(job)

	opts := options.FindOneAndUpdate().
		SetProjection(cancelJobProjection).
		SetReturnDocument(options.After)

	dbResult := backend.jobs.FindOneAndUpdate(ctx, filter, update, opts)
	if err := backend.CheckMongoErr(dbResult.Err(), ""); err != nil {
		return db.ResultCancelJob{}, err
	}

	result := db.ResultCancelJob{}
	err := dbResult.Decode(&result.BatchSummaries)
	if err != nil {
		return result, fmt.Errorf("error decoding batch record: %w", err)
	}

	return result, nil
}

var cancelJobProjection = MustCompileStaticDocument(m{
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
