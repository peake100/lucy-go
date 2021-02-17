package service

/*
This file contains all lucy rpc methods for making updates to a job stage during a job
run.
*/

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/internal/db/lucymongo"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
)

// emptyResponse will be used to respond to requests where we can use an empty response
// so we don't have to make it more than once.
var emptyResponse = new(emptypb.Empty)

// updateType is a set of consts for
type updateType string

const (
	updateTypeStart    updateType = "start"
	updateTypeProgress updateType = "progress"
	updateTypeComplete updateType = "complete"
)

// stageUpdate holds the information needed for a stage update.
type stageUpdate struct {
	// Req is the original update req from the rpc caller.
	Req proto.Message
	// StageId identifies the stage to update.
	StageId *lucy.StageID
	// Filter is the filter to use to find the document to update.
	Filter bson.D
	// Pipeline is the update aggregation pipeline to apply.
	Pipeline bson.A
	// UpdateType is what we are updating the stage to (eg progress) and is used in
	// error messages.
	UpdateType updateType
	// StatusPassList are the legal statuses for the current state of the stage. For
	// instance, to make a Progress update, the stage must be in the lucy.Status_RUNNING
	// state.
	StatusPassList []lucy.Status
	// TimeField is a field to add the modified timestamp to when updating the stage.
	TimeField string
	// FireEvents is a function supplied to fire off update events. This function will
	// be launched in it's own goroutine and may complete after the handler has exited.
	// Errors returned by fireEvents will be logged, but will otherwise not affect the
	// success of the handler.
	FireEvents fireUpdatedEventsFunc
}

// updatedStage holds updated job stage information from the result of a record update.
type updatedStage struct {
	RunCount uint32      `bson:"run_count"`
	Result   lucy.Result `bson:"result"`
}

// updatedJob holds updated job information from the result of a record update.
type updatedJob struct {
	JobId    *cerealMessages.UUID `bson:"id"`
	RunCount uint32               `bson:"run_count"`
	Progress float32              `bson:"progress"`
	Result   lucy.Result          `bson:"result"`
	// Stage is updated data from the stage that was updated.
	Stage updatedStage `bson:"-"`
}

// updatedJobRaw holds data from the updated document. This type is used for the raw
// extraction where all stages are present, and will have the stage of interest
// extracted  into Job for further processing.
type updatedJobRaw struct {
	Job    updatedJob     `bson:"-,inline"`
	Stages []updatedStage `bson:"stages"`
}

// ExtractUpdateInfo extracts the stage of stageIndex into Job and returns that value.
func (updated updatedJobRaw) ExtractUpdateInfo(stageIndex int) (updatedJob, error) {
	if updated.Stages == nil || len(updated.Stages) <= stageIndex {
		return updatedJob{}, errors.New("stage index out of bounds")
	}

	job := updated.Job
	job.Stage = updated.Stages[stageIndex]
	return job, nil
}

// updatedBatch holds data from the updated record. We'll use this data to fire off
// events.
type updatedBatch struct {
	Batch batchInfoUpdatedResult `bson:"-,inline"`
	// The specific job that was updated will be extracted into this field for
	// inspection by event handlers.
	Job updatedJob `bson:"-"`
}

// updatedBatchRaw is used to unmarshal the raw return document of the stage update.
type updatedBatchRaw struct {
	Batch updatedBatch    `bson:"-,inline"`
	Jobs  []updatedJobRaw `bson:"jobs"`
}

// Job fetches a job with the matching id from our record.
func (updated updatedBatchRaw) ExtractUpdateInfo(
	id *lucy.StageID,
) (updatedBatch, error) {
	idString := id.JobId.MustGoogle().String()
	for _, jobRaw := range updated.Jobs {
		if jobRaw.Job.JobId.MustGoogle().String() != idString {
			continue
		}

		job, err := jobRaw.ExtractUpdateInfo(int(id.StageIndex))
		if err != nil {
			return updatedBatch{}, err
		}

		batch := updated.Batch
		batch.Job = job
		return batch, nil
	}

	return updatedBatch{}, errors.New("job id not found in returned record")
}

// updateProjection is the projection we will apply to stage update results.
var updateProjection = lucymongo.MustCompileStaticDocument(m{
	"id":                    1,
	"modified":              1,
	"progress":              1,
	"run_count":             1,
	"job_count":             1,
	"pending_count":         1,
	"cancelled_count":       1,
	"running_count":         1,
	"completed_count":       1,
	"success_count":         1,
	"failure_count":         1,
	"jobs.id":               1,
	"jobs.run_count":        1,
	"jobs.progress":         1,
	"jobs.result":           1,
	"jobs.stages.run_count": 1,
	"jobs.stages.result":    1,
})

// fireUpdatedEventsFunc is the function signature for firing events after an update
// has been made.
type fireUpdatedEventsFunc = func(
	ctx context.Context, req proto.Message, updated updatedBatch, logger zerolog.Logger,
) error

// newStageUpdate creates a new stageUpdate which describes the update to be applied.
func newStageUpdate(
	stageId *lucy.StageID,
	req proto.Message,
	update interface{},
	statusPassList []lucy.Status,
	updateType updateType,
	timeField string,
	fireEvents fireUpdatedEventsFunc,
) stageUpdate {
	// Find a batch record where a job of the id exists, and that job's stages array has
	// an index matching the requested index.
	//
	// We are also going to check that the job is in an appropriate state for this
	// update. For instance, completion and progress updates are only good if teh job is
	// has a RUNNING status.
	stageField := fmt.Sprintf("stages.%v", stageId.StageIndex)
	filter := bson.D{
		{"jobs.id", stageId.JobId},
		{"jobs", m{
			"$elemMatch": bson.M{
				"id":                   stageId.JobId,
				stageField:             m{"$exists": true},
				stageField + ".status": m{"$in": statusPassList},
			},
		}},
	}

	if updateType == updateTypeStart {
		maxRetriesConstraint := bson.E{
			Key: "$expr",
			Value: m{
				"$let": m{
					"vars": m{
						"job": m{
							"$arrayElemAt": arr{
								"$jobs",
								m{
									"$indexOfArray": arr{
										"$jobs",
										m{"id": stageId.JobId},
									},
								},
							},
						},
					},
					"in": m{
						"$lte": arr{"$$job.run_count", "$$job.max_retries"},
					},
				},
			},
		}
		filter = append(filter, maxRetriesConstraint)
	}

	pipeline := lucymongo.CreateStageUpdatePipeline(stageId, update, timeField)
	updateInfo := stageUpdate{
		Req:            req,
		StageId:        stageId,
		Filter:         filter,
		Pipeline:       pipeline,
		UpdateType:     updateType,
		StatusPassList: statusPassList,
		TimeField:      timeField,
		FireEvents:     fireEvents,
	}

	return updateInfo
}

// updateJobStage generically applies a worker update to a job stage.
func (service Lucy) updateJobStage(
	ctx context.Context, update stageUpdate,
) (*emptypb.Empty, error) {
	// Execute the database update.
	decoder, err := service.updateJobStageExecuteDBRequest(ctx, update)
	if err != nil {
		return nil, err
	}

	// Handle firing update events.
	service.updateJobStageFireEvents(ctx, update, decoder)

	return emptyResponse, nil
}

// updateJobStageExecuteDBRequest handles running the update and deducing the correct
// error to return when a bad update occurs.
func (service Lucy) updateJobStageExecuteDBRequest(
	ctx context.Context, update stageUpdate,
) (decoder documentDecoder, err error) {
	// Try the update.
	opts := options.FindOneAndUpdate().
		SetProjection(updateProjection).
		SetReturnDocument(options.After)

	result := service.dbMongo.Jobs.FindOneAndUpdate(
		ctx, update.Filter, update.Pipeline, opts,
	)

	// If err is an mongo.ErrNoDocuments error, we need to check and make sure the find
	// did not fail due to the document being in the wrong status state.
	if errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return nil, service.investigateStageUpdateErr(ctx, update)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("error updating job stage: %w", result.Err())
	}

	// If it is some other error, return it.
	return result, nil
}

// investigateStageUpdateErr inspects the stage we were trying to update and attempts to
// deduce the cause of the error.
func (service Lucy) investigateStageUpdateErr(
	ctx context.Context, update stageUpdate,
) (err error) {
	// It's possible that we did not find a record because our record was in the wrong
	// state. Let's do another search without the state restriction to see if the job
	// id and stage index exist at all.
	stageField := fmt.Sprintf("stages.%v", update.StageId.StageIndex)
	update.Filter = bson.D{
		{"jobs.id", update.StageId.JobId},
		{"jobs", m{
			"$elemMatch": bson.M{
				"id":       update.StageId.JobId,
				stageField: m{"$exists": true},
			},
		}},
	}

	logger := pkmiddleware.LoggerFromCtx(ctx)

	// We're going to try this operation 3 times.
	for i := 0; i < 3; i++ {
		// Extract the status.
		findOpts := options.FindOne().SetProjection(m{"jobs.$": 1})
		findResult := service.dbMongo.Jobs.FindOne(ctx, update.Filter, findOpts)
		err = findResult.Err()

		// If there was no error once we remove the status check, that means the stage
		// is in a bad state, and we need to return an error that indicates this.
		if err == nil {
			return service.investigateStageUpdateErrJobExists(findResult, update)
		} else if errors.Is(err, mongo.ErrNoDocuments) {
			return service.errs.NewErr(
				pkerr.ErrNotFound,
				"no job stage found that matched stage_id. make sure the job"+
					" id is correct, and the stage index is not out of bounds",
				nil,
				err,
			)
		}
	}

	logger.Warn().
		Err(err).
		Msg("unexpected mongo error during status update failure")

	// Return an unknown error with the following message.
	return fmt.Errorf(
		"could not verify whether stage id was not found or stage status was"+
			" bad during stage update: %w",
		err,
	)
}

func (service Lucy) investigateStageUpdateErrJobExists(
	findResult *mongo.SingleResult, update stageUpdate,
) error {
	passListStrings := make([]string, len(update.StatusPassList))
	for j, thisPass := range update.StatusPassList {
		passListStrings[j] = thisPass.String()
	}

	// We'll use this document to extract the information we are interested in.
	document := struct {
		Jobs []struct {
			Status     lucy.Status `bson:"status"`
			RunCount   uint32      `bson:"run_count"`
			MaxRetries uint32      `bson:"max_retries"`
		} `bson:"jobs"`
	}{}

	// Decode our document into our struct.
	err := findResult.Decode(&document)
	if err != nil {
		return fmt.Errorf("error decoding document: %w", err)
	}

	// Get the projected job document.
	job := document.Jobs[0]

	// if the status of the job is cancelled, then return a lucy.ErrJobCancelled error.
	if job.Status == lucy.Status_CANCELLED {
		return service.errs.NewErr(
			lucy.ErrJobCancelled,
			fmt.Sprintf(
				"cannot apply %v update",
				update.UpdateType,
			),
			nil,
			nil,
		)
	}

	// If our run count is greater than our max retries count, then we need to return
	// lucy.ErrMaxRetriesExceeded.
	if job.RunCount > job.MaxRetries {
		return service.errs.NewErr(
			lucy.ErrMaxRetriesExceeded,
			fmt.Sprintf(
				"cannot apply %v update",
				update.UpdateType,
			),
			nil,
			nil,
		)
	}

	// Otherwise this is a bad return because the record is in a disallowed state
	return service.errs.NewErr(
		lucy.ErrInvalidStageStatus,
		fmt.Sprintf(
			"cannot apply %v update: job stage must be in one of the "+
				"following states: %v",
			update.UpdateType,
			strings.Join(passListStrings, ","),
		),
		nil,
		nil,
	)
}

func (service Lucy) updateJobStageFireEvents(
	handlerCtx context.Context, update stageUpdate, resultDecoder documentDecoder,
) {
	// Log any errors on the way out.
	logger := pkmiddleware.LoggerFromCtx(handlerCtx)
	var err error
	defer func() {
		if err == nil {
			return
		}
		logger.Error().Err(err).Msg("error firing events.")
	}()

	updatedRaw := updatedBatchRaw{}
	err = resultDecoder.Decode(&updatedRaw)
	if err != nil {
		err = fmt.Errorf("error decoding updated record: %w", err)
		return
	}

	var updated updatedBatch
	updated, err = updatedRaw.ExtractUpdateInfo(update.StageId)
	if err != nil {
		err = fmt.Errorf("error extracting updated data: %w", err)
		return
	}

	// TODO: use the resources ctx (and WaitGroup) here.
	err = update.FireEvents(context.Background(), update.Req, updated, logger)
	if err != nil {
		err = fmt.Errorf("error firing events")
	}
}

func (service Lucy) updateJobStageFireBatchUpdate(
	ctx context.Context, req proto.Message, updated updatedBatch, logger zerolog.Logger,
) {
	event := &events.BatchUpdated{
		Id:             updated.Batch.BatchId,
		JobCount:       updated.Batch.JobCount,
		Progress:       updated.Batch.Progress,
		PendingCount:   updated.Batch.PendingCount,
		CancelledCount: updated.Batch.CancelledCount,
		RunningCount:   updated.Batch.RunningCount,
		CompletedCount: updated.Batch.CompletedCount,
		SuccessCount:   updated.Batch.SuccessCount,
		FailureCount:   updated.Batch.FailureCount,
	}

	service.messenger.QueueBatchUpdated(event, logger)
}

func (service Lucy) StartStage(
	ctx context.Context, req *lucy.StartStage,
) (*empty.Empty, error) {
	updateData := struct {
		*lucy.StartStageUpdate `bson:",inline"`
		Status                 lucy.Status          `bson:"status"`
		Completed              *timestamp.Timestamp `bson:"completed"`
		Progress               float32              `bson:"progress"`
		Result                 lucy.Result          `bson:"result"`
		ResultData             *anypb.Any           `bson:"result_data"`
		Error                  *pkerr.Error         `bson:"error"`
		RunCount               bson.M               `bson:"run_count"`
	}{
		StartStageUpdate: req.Update,
		Status:           lucy.Status_RUNNING,
		Completed:        nil,
		Progress:         0.0,
		Result:           lucy.Result_NONE,
		ResultData:       nil,
		Error:            nil,
		RunCount:         m{
			"$add": arr{"$" + lucymongo.TempStageField + ".run_count", 1},
		},
	}

	update := newStageUpdate(
		req.StageId,
		req,
		updateData,
		// We can start or re-start a stage on any status but CANCELLED.
		[]lucy.Status{lucy.Status_PENDING, lucy.Status_RUNNING, lucy.Status_COMPLETED},
		updateTypeStart,
		"started",
		service.startStageFireEvents,
	)

	return service.updateJobStage(ctx, update)
}

func (service Lucy) startStageFireEvents(
	ctx context.Context, req proto.Message, updated updatedBatch, logger zerolog.Logger,
) error {
	reqStart := req.(*lucy.StartStage)

	stageEvent := &events.StageStart{
		Id: &events.StageId{
			BatchId:    updated.Batch.BatchId,
			JobId:      reqStart.StageId.JobId,
			StageIndex: reqStart.StageId.StageIndex,
		},
		RunBy:    reqStart.Update.RunBy,
		RunCount: updated.Job.Stage.RunCount,
		Modified: updated.Batch.Modified,
	}
	service.messenger.QueueStageStart(stageEvent, logger)

	// If this is not the first stage being started, exit.
	if reqStart.StageId.StageIndex != 0 {
		return nil
	}

	// Fire the Job Start event.
	jobEvent := &events.JobStart{
		Id: &events.JobId{
			BatchId: updated.Batch.BatchId,
			JobId:   updated.Job.JobId,
		},
		RunBy:    reqStart.Update.RunBy,
		RunCount: updated.Job.RunCount,
		Modified: updated.Batch.Modified,
	}
	service.messenger.QueueJobStart(jobEvent, logger)

	// Fire a batch info update
	service.updateJobStageFireBatchUpdate(ctx, req, updated, logger)

	return nil
}

func (service Lucy) ProgressStage(
	ctx context.Context, req *lucy.ProgressStage,
) (*empty.Empty, error) {
	update := newStageUpdate(
		req.StageId,
		req,
		// We can use the update object here directly since we are only updating the
		// progress field.
		req.Update,
		// We can only advance progress on a stage that is running.
		[]lucy.Status{lucy.Status_RUNNING},
		updateTypeProgress,
		"",
		service.progressStageFireEvents,
	)
	return service.updateJobStage(ctx, update)
}

func (service Lucy) progressStageFireEvents(
	ctx context.Context, req proto.Message, updated updatedBatch, logger zerolog.Logger,
) error {
	reqProgress := req.(*lucy.ProgressStage)

	// Fire off the stage progress event.
	stageEvent := &events.StageProgress{
		Id: &events.StageId{
			BatchId:    updated.Batch.BatchId,
			JobId:      reqProgress.StageId.JobId,
			StageIndex: reqProgress.StageId.StageIndex,
		},
		Progress: reqProgress.Update.Progress,
		Modified: updated.Batch.Modified,
	}
	service.messenger.QueueStageProgress(stageEvent, logger)

	// Fire off the job progress event.
	jobEvent := &events.JobProgress{
		Id: &events.JobId{
			BatchId: updated.Batch.BatchId,
			JobId:   updated.Job.JobId,
		},
		Progress: updated.Job.Progress,
		Modified: updated.Batch.Modified,
	}
	service.messenger.QueueJobProgress(jobEvent, logger)

	// Fire a batch info update
	service.updateJobStageFireBatchUpdate(ctx, req, updated, logger)

	return nil
}

func (service Lucy) CompleteStage(
	ctx context.Context, req *lucy.CompleteStage,
) (*empty.Empty, error) {
	result := lucy.Result_SUCCEEDED

	// progress is only going to be updated on a success, otherwise it will stay where
	// it is.
	var progress *float32
	if req.Update.Error != nil {
		result = lucy.Result_FAILED
	} else {
		// If there was no error, progress is set to 1.0, it is fully complete.
		progressVal := float32(1.0)
		progress = &progressVal
	}

	// Create our updateData data
	updateData := struct {
		*lucy.CompleteStageUpdate `bson:",inline"`
		Status                    lucy.Status `bson:"status"`
		Progress                  *float32    `bson:"progress,omitempty"`
		Result                    lucy.Result `bson:"result"`
	}{
		CompleteStageUpdate: req.Update,
		Status:              lucy.Status_COMPLETED,
		// Set progress to 1.0, we are done
		Progress: progress,
		Result:   result,
	}

	update := newStageUpdate(
		req.StageId,
		req,
		updateData,
		// We can only complete a stage that is running.
		[]lucy.Status{lucy.Status_RUNNING},
		updateTypeComplete,
		"completed",
		service.completeStageFireEvents,
	)
	return service.updateJobStage(ctx, update)
}

func (service Lucy) completeStageFireEvents(
	ctx context.Context, req proto.Message, updated updatedBatch, logger zerolog.Logger,
) error {
	completeReq := req.(*lucy.CompleteStage)

	// Fire stage complete event.
	stageEvent := &events.StageComplete{
		Id: &events.StageId{
			BatchId:    updated.Batch.BatchId,
			JobId:      completeReq.StageId.JobId,
			StageIndex: completeReq.StageId.StageIndex,
		},
		Result:   updated.Job.Stage.Result,
		Modified: updated.Batch.Modified,
	}
	service.messenger.QueueStageComplete(stageEvent, logger)

	// TODO: decide whether we should fire a progress = 1.0 update here or just let
	//   client applications infer that from the completion event.

	// If the job had a result, return it.
	if updated.Job.Result != lucy.Result_NONE {
		// Otherwise, fire off a job completed event.
		jobEvent := &events.JobComplete{
			Id: &events.JobId{
				BatchId: updated.Batch.BatchId,
				JobId:   completeReq.StageId.JobId,
			},
			Result:   updated.Job.Result,
			Modified: updated.Batch.Modified,
		}
		service.messenger.QueueJobComplete(jobEvent, logger)
	}

	// Fire a batch info update -- we might have a progress update, even if a completion
	// has not occurred.
	service.updateJobStageFireBatchUpdate(ctx, req, updated, logger)

	return nil
}
