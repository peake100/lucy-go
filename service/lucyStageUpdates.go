package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/lucy"
	"github.com/peake100/lucy-go/service/lucydb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
)

// we'll use this to respond to requests where we can use an empty response so we
var emptyResponse = new(emptypb.Empty)

// stageUpdate holds the information needed for a stage update.
type stageUpdate struct {
	Filter bson.M
	Pipeline bson.A
	UpdateType string
	StatusPassList []lucy.Status
	TimeField string
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
		return service.updateJobStageCheckForBadStatus(ctx, update)
	}

	// If it is some other error, return it.
	return fmt.Errorf("error updating job stage: %w", result.Err())
}

func (service Lucy) updateJobStageCheckForBadStatus(
	ctx context.Context, update stageUpdate,
) error {
	// It's possible that we did not find a record because our record was in the wrong
	// state. Let's do another search without the state restriction to see if the job
	// id and stage index exist at all.
	jobsFilter := update.Filter["jobs"].(bson.M)
	elemMatch := jobsFilter["$elemMatch"].(bson.M)
	delete(elemMatch, "status")

	// We're going to try this operation 3 times.
	for i := 0 ; i < 3 ; i++ {
		// Extract the status.
		findOpts := options.FindOne().SetProjection(m{"jobs.stages.$.status": 1})
		findResult := service.db.Jobs.FindOne(ctx, update.Filter, findOpts)

		// If there was no error once we remove the status check, that means the stage
		// is in a bad state, and we need to return an error that indicates this.
		if findResult.Err() == nil {
			passListStrings := make([]string, len(update.StatusPassList))
			for j, thisPass := range update.StatusPassList {
				passListStrings[j] = thisPass.String()
			}

			return service.errs.NewErr(
				lucy.ErrWrongJobStageStatus,
				fmt.Sprintf(
					"cannot apply %v update. job stage must be in one of the " +
						"following states: %v",
					update.UpdateType,
					strings.Join(passListStrings, ","),
				),
				nil,
				nil,
			)
			// Otherwise, if we still cannot find the stage, return the original error.
		} else if errors.Is(findResult.Err(), mongo.ErrNoDocuments) {
			return service.errs.NewErr(
				pkerr.ErrNotFound,
				"no job stage found that matched stage_id. make sure the job"+
					" id is correct, and the stage index is not pur of bounds",
				nil,
				findResult.Err(),
			)
		}
		// If we got some other error, try again.
	}

	// Return an unknown error with the following message.
	return errors.New(
		"could not verify whether stage id was not found or stage status was bad" +
			" during stage update",
	)
}

// updateJobStage is a generic function to update the job stage.
func (service Lucy) updateJobStage(
	ctx context.Context,
	stageId *lucy.StageID,
	update interface{},
	statusPassList []lucy.Status,
	updateType string,
	timeField string,
) error {
	// Find a batch record where a job of the id exists, and that job's stages array has
	// an index matching the requested index.
	//
	// We are also going to check that the job is in an appropriate state for this
	// update. For instance, completion and progress updates are only good if teh job is
	// has a RUNNING status.
	filter := m{
		"jobs.id": stageId.JobId,
		"jobs": m{
			"$elemMatch": bson.M{
				"id": stageId.JobId,
				fmt.Sprintf("stages.%v", stageId.StageIndex): m{"$exists": true},
				"status": m{
					"$in": statusPassList,
				},
			},
		},
	}

	pipeline := lucydb.CreateStageUpdatePipeline(stageId, update, timeField)
	updateInfo := stageUpdate{
		Filter:     filter,
		Pipeline:   pipeline,
		UpdateType: updateType,
		StatusPassList: statusPassList,
		TimeField: timeField,
	}

	err := service.updateJobStageHandleErr(ctx, updateInfo)
	if err != nil {
		return err
	}

	return nil
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
		RunCount 			   bson.M				`bson:"run_count"`
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
		"start",
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
		"progress",
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
	if stage.Update.Error != nil {
		result = lucy.Result_FAILED
	}

	// Create our update data
	update := struct {
		*lucy.CompleteStageUpdate `bson:",inline"`
		Status                 lucy.Status          `bson:"status"`
		Progress               float32              `bson:"progress"`
		Result                 lucy.Result          `bson:"result"`
	}{
		CompleteStageUpdate: stage.Update,
		Status:           lucy.Status_COMPLETED,
		// Set progress to 1.0, we are done
		Progress:         1.0,
		Result:           result,
	}

	err := service.updateJobStage(
		ctx,
		stage.StageId,
		update,
		// We can only complete a stage that is running.
		[]lucy.Status{lucy.Status_RUNNING},
		"complete",
		"completed",
	)
	if err != nil {
		return nil, err
	}

	return emptyResponse, nil
}
