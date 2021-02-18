package db

import (
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/pkg/lucy"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReqStageUpdate contains information needed to update a stage.
type StageUpdate struct {
	// StageId is the stage to update.
	StageId *lucy.StageID

	// TypeName is the update type name used for errors.
	TypeName UpdateTypeName

	// ValidateRunCount is whether the find should be restricted by run count.
	ValidateRunCount bool
	// ValidStatuses is the list of statuses a job may be in for this update to be
	// valid.
	ValidStatuses []lucy.Status

	// IncrementRunCount denotes whether the run_count of the stage and job should be
	// modified.
	IncrementRunCount bool
	// ModifiedTimeField is an additional field to add the modified time to, such as
	// started_at, or completed_at.
	ModifiedTimeField string

	// RunBy holds update information for the stage.run_by field.
	RunBy StageUpdateRunBy
	// Status holds update information for the stage.status field. Status must always be
	// updated.
	Status lucy.Status
	// Progress holds update information for the stage.progress field.
	Progress StageUpdateProgress
	// Result holds update information for the stage.result field.
	Result StageUpdateResult
	// ResultData holds update information for the stage.result_data field.
	ResultData StageUpdateResultData
	// Error holds update information for the stage.error field.
	Error StageUpdateError
}

// UpdateTypeName is a set of consts for
type UpdateTypeName string

const (
	UpdateTypeStart    UpdateTypeName = "start"
	UpdateTypeProgress UpdateTypeName = "progress"
	UpdateTypeComplete UpdateTypeName = "complete"
)

// StageUpdateRunBy contains stages.run_by data for update.
type StageUpdateRunBy struct {
	// Value is the value to apply to the field.
	Value string
	// Update signals that this value should be updated if true.
	Update bool
}

// StageUpdateRunBy contains stages.status data for update.
type StageUpdateStatus struct {
	// Value is the value to apply to the field.
	Value lucy.Status
	// Update signals that this value should be updated if true.
	Update bool
}

// StageUpdateProgress contains stages.progress data for update.
type StageUpdateProgress struct {
	// Value is the value to apply to the field.
	Value float32
	// Update signals that this value should be updated if true.
	Update bool
}

// StageUpdateResult contains stages.result data for update.
type StageUpdateResult struct {
	// Value is the value to apply to the field.
	Value lucy.Result
	// Update signals that this value should be updated if true.
	Update bool
}

// StageUpdateResultData contains stages.result_data data for update.
type StageUpdateResultData struct {
	// Value is the value to apply to the field.
	Value *anypb.Any
	// Update signals that this value should be updated if true.
	Update bool
}

// StageUpdateError contains stages.error data for update.
type StageUpdateError struct {
	// Value is the value to apply to the field.
	Value *pkerr.Error
	// Update signals that this value should be updated if true.
	Update bool
}
