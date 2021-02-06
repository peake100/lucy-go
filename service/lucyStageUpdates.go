package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/lucy"
	"github.com/peake100/lucy-go/service/lucydb"
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
	// StageId identifies the stage to update.
	StageId *lucy.StageID
}

// updateJobStage is a generic function to update the job stage.
func (service Lucy) updateJobStage(
	ctx context.Context,
	stageId *lucy.StageID,
	update interface{},
	statusPassList []lucy.Status,
	updateType updateType,
	timeField string,
) error {
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

	pipeline := lucydb.CreateStageUpdatePipeline(stageId, update, timeField)
	updateInfo := stageUpdate{
		Filter:         filter,
		Pipeline:       pipeline,
		UpdateType:     updateType,
		StatusPassList: statusPassList,
		TimeField:      timeField,
		StageId:        stageId,
	}

	err := service.updateJobStageHandleErr(ctx, updateInfo)
	if err != nil {
		return err
	}

	return nil
}

// updateJobStageHandleErr handles running the update and deducing the correct error
// to return when a bad update occurs.
func (service Lucy) updateJobStageHandleErr(
	ctx context.Context, update stageUpdate,
) (err error) {
	// Try the update.
	opts := options.FindOneAndUpdate().SetProjection(m{"_id": 1})
	result := service.db.Jobs.FindOneAndUpdate(
		ctx, update.Filter, update.Pipeline, opts,
	)

	if result.Err() == nil {
		// If there was no error, return. We are good to go.
		return nil
	}

	// If err is an mongo.ErrNoDocuments error, we need to check and make sure the find
	// did not fail due to the document being in the wrong status state.
	if errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return service.investigateStageUpdateErr(ctx, update)
	}

	// If it is some other error, return it.
	return fmt.Errorf("error updating job stage: %w", result.Err())
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
		findResult := service.db.Jobs.FindOne(ctx, update.Filter, findOpts)
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

func (service Lucy) StartStage(
	ctx context.Context, stage *lucy.StartStage,
) (*empty.Empty, error) {
	update := struct {
		*lucy.StartStageUpdate `bson:",inline"`
		Status                 lucy.Status          `bson:"status"`
		Completed              *timestamp.Timestamp `bson:"completed"`
		Progress               float32              `bson:"progress"`
		Result                 lucy.Result          `bson:"result"`
		ResultData             *anypb.Any           `bson:"result_data"`
		Error                  *pkerr.Error         `bson:"error"`
		RunCount               bson.M               `bson:"run_count"`
	}{
		StartStageUpdate: stage.Update,
		Status:           lucy.Status_RUNNING,
		Completed:        nil,
		Progress:         0.0,
		Result:           lucy.Result_NONE,
		ResultData:       nil,
		Error:            nil,
		RunCount:         m{"$add": arr{"$" + lucydb.TempStageField + ".run_count", 1}},
	}

	err := service.updateJobStage(
		ctx,
		stage.StageId,
		update,
		// We can start or re-start a stage on any status but CANCELLED.
		[]lucy.Status{lucy.Status_PENDING, lucy.Status_RUNNING, lucy.Status_COMPLETED},
		updateTypeStart,
		"started",
	)
	if err != nil {
		return nil, err
	}

	return emptyResponse, nil
}

func (service Lucy) ProgressStage(
	ctx context.Context, stage *lucy.ProgressStage,
) (*empty.Empty, error) {

	err := service.updateJobStage(
		ctx,
		stage.StageId,
		// We can use the update object here directly since we are only updating the
		// progress field.
		stage.Update,
		// We can only advance progress on a stage that is running.
		[]lucy.Status{lucy.Status_RUNNING},
		updateTypeProgress,
		"",
	)
	if err != nil {
		return nil, err
	}

	return emptyResponse, nil
}

func (service Lucy) CompleteStage(
	ctx context.Context, stage *lucy.CompleteStage,
) (*empty.Empty, error) {
	result := lucy.Result_SUCCEEDED

	// progress is only going to be updated on a success, otherwise it will stay where
	// it is.
	var progress *float32
	if stage.Update.Error != nil {
		result = lucy.Result_FAILED
	} else {
		// If there was no error, progress is set to 1.0, it is fully complete.
		progressVal := float32(1.0)
		progress = &progressVal
	}

	// Create our update data
	update := struct {
		*lucy.CompleteStageUpdate `bson:",inline"`
		Status                    lucy.Status `bson:"status"`
		Progress                  *float32    `bson:"progress,omitempty"`
		Result                    lucy.Result `bson:"result"`
	}{
		CompleteStageUpdate: stage.Update,
		Status:              lucy.Status_COMPLETED,
		// Set progress to 1.0, we are done
		Progress: progress,
		Result:   result,
	}

	err := service.updateJobStage(
		ctx,
		stage.StageId,
		update,
		// We can only complete a stage that is running.
		[]lucy.Status{lucy.Status_RUNNING},
		updateTypeComplete,
		"completed",
	)
	if err != nil {
		return nil, err
	}

	return emptyResponse, nil
}

// cancelBatchMongoPipeline is the UpdateMany pipeline to cancel a job.
var cancelBatchMongoPipeline = lucydb.MustCompileStaticPipeline(
	lucydb.CancelJobsPipeline(nil),
)

func (service Lucy) CancelBatches(
	ctx context.Context, batches *lucy.CancelBatches,
) (*empty.Empty, error) {
	filter := m{"id": m{"$in": batches.BatchIds}}
	update := cancelBatchMongoPipeline

	result, err := service.db.Jobs.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, fmt.Errorf("error running cancellation pipeline: %w", err)
	}

	batchesRequested := int64(len(batches.BatchIds))
	if result.MatchedCount != batchesRequested {
		return nil, service.errs.NewErr(
			pkerr.ErrNotFound,
			fmt.Sprintf(
				"%v batches found and cancelled, %v batches could not be found",
				result.MatchedCount,
				batchesRequested,
			),
			nil,
			nil,
		)
	}

	return emptyResponse, nil
}

func (service Lucy) CancelJob(
	ctx context.Context, job *cerealMessages.UUID,
) (*empty.Empty, error) {
	filter := m{"jobs.id": job}
	update := lucydb.CancelJobsPipeline(job)

	result := service.db.Jobs.FindOneAndUpdate(ctx, filter, update)
	if err := service.CheckMongoErr(result.Err(), ""); err != nil {
		return nil, err
	}

	return new(emptypb.Empty), nil
}
