package db

import (
	"context"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/pkg/lucy"
)

// Backend is the interface a db connection type must implement to back the
// lucy service.
type Backend interface {
	// GetBatch fetches a batch from the database.
	GetBatch(ctx context.Context, batchId *cereal.UUID) (ResultGetBatch, error)

	// ListBatches returns a cursor to fetch batches from most-recently added to
	// least-recently added.
	ListBatches(ctx context.Context) (ListBatchesCursor, error)

	// GetBatchJobs fetches a batch's jobs from the database.
	GetBatchJobs(
		ctx context.Context, batchId *cereal.UUID,
	) (ResultGetBatchJobs, error)

	// CancelBatches applies a cancellation status to all non-success jobs in the passed
	// batches.
	CancelBatches(
		ctx context.Context, batches *lucy.CancelBatches,
	) (CancelBatchResultsCursor, error)

	// GetJob fetches a job from the database.
	GetJob(ctx context.Context, jobId *cereal.UUID) (ResultGetJob, error)

	// CreateBatch creates a new batch.
	CreateBatch(ctx context.Context, batch *lucy.NewBatch) (ResultCreateBatch, error)

	// CreateJobs creates new job(s) records.
	CreateJobs(ctx context.Context, jobs *lucy.NewJobs) (ResultCreateJobs, error)

	// CancelJob applies a cancellation status to a single job.
	CancelJob(ctx context.Context, job *cereal.UUID) (ResultCancelJob, error)

	// UpdateStage applies an update to a job stage.
	UpdateStage(ctx context.Context, update StageUpdate) (ResultWorkerUpdate, error)

	// Disconnect releases any resources the database connector currently has open.
	Disconnect(ctx context.Context) error
}

// ListBatchesCursor yields a new batch record each time Next() is called
type ListBatchesCursor interface {
	// Next returns a cancelled batch summary data or io.EOF if all relevant documents
	// have been returned.
	Next(ctx context.Context) (*lucy.Batch, error)
}

// CancelBatchResultsCursor yields a new cancelled batch summary each time Next() is
// called.
type CancelBatchResultsCursor interface {
	// Next returns a cancelled batch summary data or io.EOF if all relevant documents
	// have been returned.
	Next(ctx context.Context) (ResultCancelBatch, error)
}

// NewBackendFunc is a function that creates a new Backend.
type NewBackendFunc = func(
	ctx context.Context, errGen *pkerr.ErrorGenerator,
) (Backend, error)

// BackendType is an enum-like string value for valid backend specifiers in
// configuration contexts.
type BackendType string

const (
	// BackendTypeMongo denotes a MongoDB backend
	BackendTypeMongo = "MONGODB"
	// BackendTypeSQLite denotes an SQLite backend.
	BackendTypeSQLite = "SQLITE"
)
