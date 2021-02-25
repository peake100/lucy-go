package messaging

import (
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/lucy-go/pkg/lucy/events"
)

// EventType is the sting at the end of a an event routing key to indicate what kind
// of event this is.
type EventType string

const (
	// EventTypeCreated is the tag fired when a batch or job is created.
	EventTypeCreated = "CREATED"
	// EventTypeStart is the tag fired when a job or job stage is started.
	EventTypeStart = "START"
	// EventTypeProgress is the tag fired when a job or job stage's progress is
	// updated.
	EventTypeProgress = "PROGRESS"
	// EventTypeComplete is the tag fired when a job or job stage is completed.
	EventTypeComplete = "COMPLETE"
	// EventTypeUpdated is used when a batch is updated from a job stage update / job
	// creation, etc.
	EventTypeUpdated = "UPDATED"
	// EventTypeComplete is the tag fired when a job is cancelled.
	EventTypeCancelled = "CANCELLED"
)

// WorkerQueueName returns the name of the worker queue that will be created for a
// given job type.
func WorkerQueueName(jobType string) string {
	return "JOBS." + jobType
}

// StageEventKey creates a stage routing event key of a given stage.
func StageEventKey(stageId *events.StageId, eventType string) string {
	return fmt.Sprintf(
		"batch.%v.job.%v.stage.%v.%v",
		stageId.BatchId.MustGoogle().String(),
		stageId.JobId.MustGoogle().String(),
		stageId.StageIndex,
		eventType,
	)
}

// JobEventKey creates a job event routing key for a given job.
func JobEventKey(jobId *events.JobId, eventType string) string {
	return fmt.Sprintf(
		"batch.%v.job.%v.%v",
		jobId.BatchId.MustGoogle().String(),
		jobId.JobId.MustGoogle().String(),
		eventType,
	)
}

// BatchEventKey creates a batch event routing key for a given job.
func BatchEventKey(batchId *cereal.UUID, eventType string) string {
	return fmt.Sprintf(
		"batch.%v.%v",
		batchId.MustGoogle().String(),
		eventType,
	)
}
