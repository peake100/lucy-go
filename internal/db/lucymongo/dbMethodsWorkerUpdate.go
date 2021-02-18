package lucymongo

import (
	"context"
	"errors"
	"fmt"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/gRPEAKEC-go/pkmiddleware"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
)

func (backend Backend) UpdateStage(
	ctx context.Context, req db.StageUpdate,
) (db.ResultWorkerUpdate, error) {
	update := newStageUpdate(req)

	// Try the update.
	opts := options.FindOneAndUpdate().
		SetProjection(updateProjection).
		SetReturnDocument(options.After)

	dbResult := backend.Jobs.FindOneAndUpdate(
		ctx, update.Filter, update.Pipeline, opts,
	)

	// If err is an mongo.ErrNoDocuments error, we need to check and make sure the find
	// did not fail due to the document being in the wrong status state.
	if errors.Is(dbResult.Err(), mongo.ErrNoDocuments) {
		return db.ResultWorkerUpdate{}, backend.investigateStageUpdateErr(ctx, update)
	}

	if dbResult.Err() != nil {
		return db.ResultWorkerUpdate{}, fmt.Errorf(
			"error updating job stage: %w", dbResult.Err(),
		)
	}

	decoded := updatedBatchRaw{}
	err := dbResult.Decode(&decoded)
	if err != nil {
		return db.ResultWorkerUpdate{}, fmt.Errorf(
			"error decoding updated document: %v", err,
		)
	}

	result, err := decoded.ExtractUpdateInfo(req.StageId)
	if err != nil {
		return db.ResultWorkerUpdate{}, fmt.Errorf(
			"error extracting updated job info: %w", err,
		)
	}

	// If it is some other error, return it.
	return result, nil
}

// stageUpdate holds the information needed for a stage update.
type stageUpdate struct {
	// Req is the original update req from the rpc caller.
	Req db.StageUpdate
	// Filter is the filter to use to find the document to update.
	Filter bson.D
	// Pipeline is the update aggregation pipeline to apply.
	Pipeline bson.A
}

// updatedJobRaw holds data from the updated document. This type is used for the raw
// extraction where all stages are present, and will have the stage of interest
// extracted  into Job for further processing.
type updatedJobRaw struct {
	Job    db.ResultWorkerUpdatedJob     `bson:"-,inline"`
	Stages []db.ResultWorkerUpdatedStage `bson:"stages"`
}

// ExtractStageInfo extracts the stage of stageIndex into Job and returns that value.
func (updated updatedJobRaw) ExtractStageInfo(
	stageIndex int,
) (db.ResultWorkerUpdatedStage, error) {
	if updated.Stages == nil || len(updated.Stages) <= stageIndex {
		return db.ResultWorkerUpdatedStage{}, errors.New(
			"stage index out of bounds",
		)
	}

	return updated.Stages[stageIndex], nil
}

// updatedBatchRaw is used to unmarshal the raw return document of the stage update.
type updatedBatchRaw struct {
	Batch db.ResultBatchSummaries `bson:"-,inline"`
	Jobs  []updatedJobRaw         `bson:"jobs"`
}

// Job fetches a job with the matching id from our record.
func (updated updatedBatchRaw) ExtractUpdateInfo(
	id *lucy.StageID,
) (result db.ResultWorkerUpdate, err error) {
	idString := id.JobId.MustGoogle().String()
	for _, jobRaw := range updated.Jobs {
		if jobRaw.Job.Id.MustGoogle().String() != idString {
			continue
		}

		stage, err := jobRaw.ExtractStageInfo(int(id.StageIndex))
		if err != nil {
			return db.ResultWorkerUpdate{}, err
		}

		// Build the result from the extracted data.
		result.StageId = id
		result.Batch = updated.Batch
		result.Job = jobRaw.Job
		result.Stage = stage

		return result, nil
	}

	return db.ResultWorkerUpdate{}, errors.New("job id not found in returned record")
}

// updateProjection is the projection we will apply to stage update results.
var updateProjection = MustCompileStaticDocument(m{
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
	"jobs.stages.run_by":    1,
	"jobs.stages.run_count": 1,
	"jobs.stages.progress":  1,
	"jobs.stages.result":    1,
})

// newStageUpdate creates a new stageUpdate which describes the update to be applied.
func newStageUpdate(
	req db.StageUpdate,
) stageUpdate {
	// Find a batch record where a job of the id exists, and that job's stages array has
	// an index matching the requested index.
	//
	// We are also going to check that the job is in an appropriate state for this
	// update. For instance, completion and progress updates are only good if teh job is
	// has a RUNNING status.
	stageField := fmt.Sprintf("stages.%v", req.StageId.StageIndex)
	filter := bson.D{
		{"jobs.id", req.StageId.JobId},
		{"jobs", m{
			"$elemMatch": bson.M{
				"id":                   req.StageId.JobId,
				stageField:             m{"$exists": true},
				stageField + ".status": m{"$in": req.ValidStatuses},
			},
		}},
	}

	// If we need to validate the run count, add the following expression to the filter.
	if req.ValidateRunCount {
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
										m{"id": req.StageId.JobId},
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

	stageUpdateData := newStageUpdateData(req)

	pipeline := CreateStageUpdatePipeline(
		req.StageId, stageUpdateData, req.ModifiedTimeField,
	)
	updateInfo := stageUpdate{
		Req:      req,
		Filter:   filter,
		Pipeline: pipeline,
	}

	return updateInfo
}

// newStageUpdateData created a bson document with the requested data.
func newStageUpdateData(req db.StageUpdate) bson.M {
	// Make a map with the correct capacity for our update.
	update := make(bson.M, 8)

	update["status"] = req.Status

	if req.RunBy.Update {
		update["run_by"] = req.RunBy.Value
	}

	if req.Progress.Update {
		update["progress"] = req.Progress.Value
	}

	if req.Result.Update {
		update["result"] = req.Result.Value
	}

	if req.ResultData.Update {
		update["result_data"] = req.ResultData.Value
	}

	if req.Error.Update {
		update["error"] = req.Error.Value
	}

	if req.ModifiedTimeField != "" {
		update[req.ModifiedTimeField] = "$$NOW"
	}

	if req.IncrementRunCount {
		update["run_count"] = m{
			"$add": arr{"$" + TempStageField + ".run_count", 1},
		}
	}

	return update
}

// investigateStageUpdateErr inspects the stage we were trying to update and attempts to
// deduce the cause of the error.
func (backend Backend) investigateStageUpdateErr(
	ctx context.Context, update stageUpdate,
) (err error) {
	// It's possible that we did not find a record because our record was in the wrong
	// state. Let's do another search without the state restriction to see if the job
	// id and stage index exist at all.
	stageField := fmt.Sprintf("stages.%v", update.Req.StageId.StageIndex)
	update.Filter = bson.D{
		{"jobs.id", update.Req.StageId.JobId},
		{"jobs", m{
			"$elemMatch": bson.M{
				"id":       update.Req.StageId.JobId,
				stageField: m{"$exists": true},
			},
		}},
	}

	logger := pkmiddleware.LoggerFromCtx(ctx)

	// We're going to try this operation 3 times.
	for i := 0; i < 3; i++ {
		// Extract the status.
		findOpts := options.FindOne().SetProjection(m{"jobs.$": 1})
		findResult := backend.Jobs.FindOne(ctx, update.Filter, findOpts)
		err = findResult.Err()

		// If there was no error once we remove the status check, that means the stage
		// is in a bad state, and we need to return an error that indicates this.
		if err == nil {
			return backend.investigateStageUpdateErrJobExists(findResult, update)
		} else if errors.Is(err, mongo.ErrNoDocuments) {
			return backend.ErrsGen.NewErr(
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

func (backend Backend) investigateStageUpdateErrJobExists(
	findResult *mongo.SingleResult, update stageUpdate,
) error {
	passListStrings := make([]string, len(update.Req.ValidStatuses))
	for j, thisPass := range update.Req.ValidStatuses {
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
		return backend.ErrsGen.NewErr(
			lucy.ErrJobCancelled,
			fmt.Sprintf(
				"cannot apply %v update",
				update.Req.TypeName,
			),
			nil,
			nil,
		)
	}

	// If our run count is greater than our max retries count, then we need to return
	// lucy.ErrMaxRetriesExceeded.
	if job.RunCount > job.MaxRetries {
		return backend.ErrsGen.NewErr(
			lucy.ErrMaxRetriesExceeded,
			fmt.Sprintf(
				"cannot apply %v update",
				update.Req.TypeName,
			),
			nil,
			nil,
		)
	}

	// Otherwise this is a bad return because the record is in a disallowed state
	return backend.ErrsGen.NewErr(
		lucy.ErrInvalidStageStatus,
		fmt.Sprintf(
			"cannot apply %v update: job stage must be in one of the "+
				"following states: %v",
			update.Req.TypeName,
			strings.Join(passListStrings, ","),
		),
		nil,
		nil,
	)
}
