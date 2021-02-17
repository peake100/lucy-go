package db

import (
	"context"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/pkg/lucy"
)

// Connection is the interface a db connection type must implement to back the
// lucy service.
type Connection interface {
	// GetBatch fetches a batch from the database.
	GetBatch(ctx context.Context, batchId *cerealMessages.UUID) (ResultGetBatch, error)

	// GetBatchJobs fetches a batch's jobs from the database.
	GetBatchJobs(
		ctx context.Context, batchId *cerealMessages.UUID,
	) (ResultGetBatchJobs, error)

	// CancelBatches applies a cancellation status to all non-success jobs in the passed
	// batches.
	CancelBatches(
		ctx context.Context, batches *lucy.CancelBatches,
	) (CancelBatchResultsCursor, error)

	// GetJob fetches a job from the database.
	GetJob(ctx context.Context, jobId *cerealMessages.UUID) (ResultGetJob, error)

	// CreateBatch creates a new batch.
	CreateBatch(ctx context.Context, batch *lucy.NewBatch) (ResultCreateBatch, error)

	// CreateJobs creates new job(s) records.
	CreateJobs(ctx context.Context, jobs *lucy.NewJobs) (ResultCreateJobs, error)

	// CancelJob applies a cancellation status to a single job.
	CancelJob(ctx context.Context, job *cerealMessages.UUID) (ResultCancelJob, error)
}

// CancelBatchResultsCursor yields a new cancelled batch summary each time Next() is
// called.
type CancelBatchResultsCursor interface {
	// Next returns a cancelled batch summary data or io.EOF if all relevant documents
	// have been returned.
	Next(ctx context.Context) (ResultCancelBatch, error)
}

// NewConnectionFunc is a function that creates a new Connection.
type NewConnectionFunc = func(
	ctx context.Context, errGen *pkerr.ErrorGenerator,
) (Connection, error)
