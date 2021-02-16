package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"github.com/peake100/rogerRabbit-go/roger"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
)

// CreateJobs implements lucy.LucyServer.
func (service Lucy) CreateJobs(
	ctx context.Context, jobs *lucy.NewJobs,
) (created *lucy.CreatedJobs, err error) {
	// We're going to update the db and declare job queues concurrently.
	logger := pkmiddleware.LoggerFromCtx(ctx)
	dbResult := make(chan error, 1)
	declareQueuesResult := make(chan error, 1)
	var updatedData batchInfoUpdatedResult

	go func() {
		createdInfo, batchInfo, dbErr := service.createJobsUpdateDBBatchRecord(
			ctx, jobs,
		)
		created = createdInfo
		if dbErr != nil {
			dbResult <- dbErr
			return
		}
		updatedData = batchInfo
		dbResult <- nil
	}()

	go func() {
		declareQueuesResult <- service.createJobsDeclareWorkerQueues(ctx, jobs)
	}()

	select {
	case err = <-dbResult:
	case <-ctx.Done():
		return nil, fmt.Errorf("db upadate interrupted: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("error upating db record: %w", err)
	}

	select {
	case err = <-declareQueuesResult:
	case <-ctx.Done():
		return nil, fmt.Errorf("job queue creation interrupted: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("error creating job queues: %w", err)
	}

	// Fire off our creation events (these just get queued to be sent in the
	// background).
	service.createJobsSendEvents(jobs.Batch, created, updatedData, logger)

	// If both procedures succeed, then we are going to queue job messages.
	err = service.createJobsQueueWorkerMessages(ctx, jobs, created)
	if err != nil {
		return nil, fmt.Errorf(
			"error queueing job messages to workers: %w", err,
		)
	}

	// Return the created message.
	return created, nil
}

// createJobProjection is the projection we will apply to job creation.
var createJobProjection = db.MustCompileStaticDocument(m{
	"id":              1,
	"modified":        1,
	"progress":        1,
	"job_count":       1,
	"pending_count":   1,
	"cancelled_count": 1,
	"running_count":   1,
	"completed_count": 1,
	"success_count":   1,
	"failure_count":   1,
})

// batchInfoUpdatedResult stores the batch-summary level info from a db update.
type batchInfoUpdatedResult struct {
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

// createJobsUpdateDBBatchRecord updates the batch record in the db with the new jobs
func (service Lucy) createJobsUpdateDBBatchRecord(
	ctx context.Context, jobs *lucy.NewJobs,
) (*lucy.CreatedJobs, batchInfoUpdatedResult, error) {
	// CurrentUpdate the batch.
	filter := bson.M{"id": jobs.Batch}

	// We need to keep track of the job id's we make so we return them in the same
	// jobQueueOrder that the jobs were sent.
	jobIds := make([]*cerealMessages.UUID, len(jobs.Jobs))
	for i := range jobs.Jobs {
		recordId, err := db.NewRecordId()
		if err != nil {
			return nil, batchInfoUpdatedResult{}, fmt.Errorf(
				"error creating record: %w", err,
			)
		}
		jobIds[i] = recordId
	}

	// Create the update pipeline for adding the jobs to the batch record.
	updatePipeline := db.CreateAddJobPipeline(jobs, jobIds)

	// Return an emtpy record.
	opts := new(options.FindOneAndUpdateOptions).
		SetProjection(createJobProjection).
		SetReturnDocument(options.After)

	result := service.db.Jobs.FindOneAndUpdate(ctx, filter, updatePipeline, opts)
	err := service.CheckMongoErr(result.Err(), "batch id not found")
	if err != nil {
		return nil, batchInfoUpdatedResult{}, err
	}

	created := &lucy.CreatedJobs{Ids: jobIds}
	batchInfo := batchInfoUpdatedResult{}
	err = result.Decode(&batchInfo)
	if err != nil {
		return nil, batchInfoUpdatedResult{}, fmt.Errorf(
			"error decoding return document: %w", err,
		)
	}

	return created, batchInfo, nil
}

// createJobsDeclareWorkerQueues declares and binds queues on the message broker for the
// job type if they have not yet been created. Declarations are done concurrently.
func (service Lucy) createJobsDeclareWorkerQueues(
	ctx context.Context, jobs *lucy.NewJobs,
) (err error) {
	// Create a set of unique job types.
	jobTypes := make(map[string]struct{})
	for _, thisJob := range jobs.Jobs {
		jobTypes[thisJob.Type] = struct{}{}
	}

	// Errors will be returned on this channel.
	errChan := make(chan error, 1)

	// This WaitGroup will be closed when all declarations are done.
	complete := new(sync.WaitGroup)
	for jobType := range jobTypes {
		complete.Add(1)
		go service.createJobsDeclareJobQueueSingle(jobType, complete, errChan)
	}

	// Close the error channel when all declarations have completed.
	go func() {
		defer close(errChan)
		complete.Wait()
	}()

	// Collect results.
	var declareErr error
	// Pull from the errChan until a non-
	for ok := true; ok; {
		select {
		case declareErr, ok = <-errChan:
			// If the channel has been closed, break out.
			if !ok {
				break
			}
			// Otherwise set err to this error.
			err = declareErr
		case <-ctx.Done():
			err = ctx.Err()
			break
		}
	}

	// Return error
	return err
}

// createJobsSendEvents fires off job creation events.
func (service Lucy) createJobsSendEvents(
	batchId *cerealMessages.UUID,
	created *lucy.CreatedJobs,
	updatedData batchInfoUpdatedResult,
	logger zerolog.Logger,
) {
	// Iterate over each job id and create an event for it.
	for _, thisJobId := range created.Ids {
		event := &events.JobCreated{
			Id: &events.JobId{
				BatchId: batchId,
				JobId:   thisJobId,
			},
			Modified: updatedData.Modified,
		}

		service.messenger.QueueJobCreated(event, logger)
	}

	// Fire off a batch updated event for the new batch summaries.
	event := &events.BatchUpdated{
		Id:             updatedData.BatchId,
		JobCount:       updatedData.JobCount,
		Progress:       updatedData.Progress,
		PendingCount:   updatedData.PendingCount,
		CancelledCount: updatedData.CancelledCount,
		RunningCount:   updatedData.RunningCount,
		CompletedCount: updatedData.CompletedCount,
		SuccessCount:   updatedData.SuccessCount,
		FailureCount:   updatedData.FailureCount,
		Modified:       updatedData.Modified,
	}
	service.messenger.QueueBatchUpdated(event, logger)
}

// createJobsDeclareJobQueueSingle creates a single job queue and reports the error.
func (service Lucy) createJobsDeclareJobQueueSingle(
	jobType string, done *sync.WaitGroup, result chan<- error,
) {
	// Decrement the work group on our way out.
	defer done.Done()

	// Declare the queue, exit if there was not error.
	err := service.messenger.DeclareJobQueue(jobType)
	if err == nil {
		return
	}

	// Add some error context.
	err = fmt.Errorf(
		"error declaring job queue for job typee '%v': %w",
		jobType,
		err,
	)

	// We only need to send an error if someone else has not already.
	select {
	case result <- err:
	default:
	}
}

// jobQueueOrder holds information for queueing a job to be published on a worker queue.
type jobQueueOrder struct {
	Id      *cerealMessages.UUID
	JobType string
}

// jobQueueResult holds result values from queuing a job to be published on a worker
// queue.
type jobQueueResult struct {
	Order       jobQueueOrder
	Publication *roger.Publication
	Err         error
}

// createJobsQueueWorkerMessages queues created jobs to their relevant worker queues.
func (service Lucy) createJobsQueueWorkerMessages(
	ctx context.Context, req *lucy.NewJobs, created *lucy.CreatedJobs,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	orders := make(chan jobQueueOrder, len(req.Jobs))
	defer close(orders)
	results := make(chan jobQueueResult, len(req.Jobs))

	// Launch a routine to send orders to the broker.
	go service.createJobsQueueWorkerMessagesSendOrders(ctx, orders, results)

	// Queue all the initial orders.
	for i, thisJob := range req.Jobs {
		orders <- jobQueueOrder{
			Id:      created.Ids[i],
			JobType: thisJob.Type,
		}
	}

	err := service.createJobsQueueWorkerMessagesHandleResults(
		results, orders, len(req.Jobs),
	)
	if err != nil {
		return fmt.Errorf("error publishing messages: %w", err)
	}

	// Return the error.
	return nil
}

// createJobsQueueWorkerMessagesSendOrders pulls message orders from orders, queues the
// job to be sent to the broker, then returns the result to jobQueueResult.
//
// This method exits when orders is closed.
func (service Lucy) createJobsQueueWorkerMessagesSendOrders(
	ctx context.Context, orders <-chan jobQueueOrder, results chan<- jobQueueResult,
) {
	for thisOrder := range orders {
		publication, err := service.messenger.QueueJob(
			ctx, thisOrder.Id, thisOrder.JobType,
		)

		results <- jobQueueResult{
			Order:       thisOrder,
			Publication: publication,
			Err:         err,
		}
	}
}

// createJobsQueueWorkerMessagesHandleResults handles the results of job message
// publication. This message exits when
func (service Lucy) createJobsQueueWorkerMessagesHandleResults(
	results <-chan jobQueueResult, orders chan<- jobQueueOrder, count int,
) error {
	successes := 0

	// Range over our results.
	for thisResult := range results {
		if thisResult.Err != nil {
			return fmt.Errorf("error queing publication: %w", thisResult.Err)
		}

		// Wait on the confirmation result from the broker.
		err := thisResult.Publication.WaitOnConfirmation()

		// If there is no error, increment our success count and continue.
		if err == nil {
			successes++
			// If all publications have succeeded, exit.
			if successes == count {
				break
			}
			continue
		}

		// If this was a nack error, try again (this will jobQueueResult orders
		// results-of-jobQueueOrder message publication, but there's not much we can
		// do about that).
		if errors.Is(err, roger.ErrProducerNack{}) {
			orders <- thisResult.Order
			continue
		}

		// If there was another type of error, return immediately.
		return fmt.Errorf("error publising message: %w", err)
	}

	return nil
}
