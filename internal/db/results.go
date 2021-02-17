package db

import (
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/lucy-go/pkg/lucy"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ResultCreateBatch returns the result for creating a batch.
type ResultCreateBatch struct {
	// Created is the created record info.
	Created *lucy.CreatedBatch
	// Modified is the updated modified time for the record.
	Modified *timestamppb.Timestamp
}

// ResultBatchSummaries stores the batch-summary level info from a dbMongo update.
type ResultBatchSummaries struct {
	BatchId        *cerealMessages.UUID   `bson:"id"`
	Modified       *timestamppb.Timestamp `bson:"modified"`
	Progress       float32                `bson:"progress"`
	JobCount       uint32                 `bson:"job_count"`
	PendingCount   uint32                 `bson:"pending_count"`
	CancelledCount uint32                 `bson:"cancelled_count"`
	RunningCount   uint32                 `bson:"running_count"`
	CompletedCount uint32                 `bson:"completed_count"`
	SuccessCount   uint32                 `bson:"success_count"`
	FailureCount   uint32                 `bson:"failure_count"`
}

// ResultCreateJobs stores the result of a jobs creation db call(s).
type ResultCreateJobs struct {
	// Jobs holds the *lucy.CreatedJobs objects for return to the rpc caller.
	Jobs *lucy.CreatedJobs
	// BatchSummaries holds the batch summary info.
	BatchSummaries ResultBatchSummaries
}

// ResultGetBatch stores the result of a batch fetch db call.
type ResultGetBatch struct {
	// Batch is the batch being returned to the rpc caller.
	Batch *lucy.Batch
}

// ResultGetJob stores the result of a job fetch db call.
type ResultGetJob struct {
	// Job is the job being returned to the rpc caller.
	Job *lucy.Job
}

// ResultGetBatchJobs stores the result of a batch jobs fetch db call.
type ResultGetBatchJobs struct {
	// Jobs is the batch jobs being returned to the rpc caller.
	Jobs *lucy.BatchJobs
}

// ResultCancelBatch stores the result of cancelling a list of batches.
type ResultCancelBatch struct {
	// BatchSummaries contains the batch summaries of the cancelled batch.
	BatchSummaries ResultBatchSummaries
	// JobIds contains the id's of the cancelled jobs. Non-cancelled jobs should
	// have their id's omitted from this operation.
	JobIds []*cerealMessages.UUID
}

// ResultCancelJob stores the result of cancelling a single job.
type ResultCancelJob struct {
	// BatchSummaries contains the batch summaries of the cancelled batch.
	BatchSummaries ResultBatchSummaries
}
