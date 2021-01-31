package lucydb

import (
	"github.com/peake100/lucy-go/lucy"
	"go.mongodb.org/mongo-driver/bson"
)

// Make these simple aliases to make our updates a little more readable.
type m = bson.M
type arr = bson.A

// TempJobField is the temporary field on the batch document we are going to store the
// job we want to update in. The value will be put back in the jobs array and this field
// removed before the pipeline completes.
const TempJobField = "jobToUpdate"

// TempStageField is the temporary field on the batch document we are going to store the
// job stage we want to update in. The value will be put back in the job.stages array
// and this field removed before the pipeline completes.
const TempStageField = "stageToUpdate"

// updateJobSummaries is the bson expression to summarize the job status and
// result based on stage statuses and results.
var updateJobSummaries = m{
	"$set": m{
		TempJobField: m{
			// We're going to reduce on the job.stages field to build a temporary object
			// with 'result' and 'status' fields that we will then merge into our job
			// document to update it.
			"$mergeObjects": arr{
				"$" + TempJobField,
				m{
					"$reduce": m{
						"input": "$" + TempJobField + ".stages",
						"initialValue": m{
							// Start with status = COMPLETED and result = SUCCEEDED.
							// See comments on updateStageStatusSwitch and
							// updateStageResultSwitch for an explanation of this
							// starting state.
							"status": lucy.Status_COMPLETED,
							"result": lucy.Result_SUCCEEDED,
							"run_count": 0,
						},
						"in": m{
							"status": updateStageStatusSwitch,
							"result": updateStageResultSwitch,
							"run_count": m{
								"$max": arr{"$$this.run_count", "$$value.run_count"},
							},
						},
					},
				},
			},
		},
	},
}

// updateStageStatusSwitch is a switch expression used in updateJobSummaries to
// update the stage.status field.
var updateStageStatusSwitch = m{
	// This switch happens inside a reduce. "$$this" is the current stage object we are
	// inspecting and $$value is the update object we are building to merge into our
	// stage after the reduce completes.
	"$switch": m{
		"branches": arr{
			// If EITHER the current status value or this stage's value is CANCELLED,
			// we use CANCELLED. Any single cancelled stage results in a cancelled job,
			// regardless of other stages.
			m{
				"case": m{"$eq": arr{"$$this.status", lucy.Status_CANCELLED}},
				"then": "$$this.status",
			},
			m{
				"case": m{"$eq": arr{"$$value.status", lucy.Status_CANCELLED}},
				"then": "$$value.status",
			},

			// Otherwise, if any stage is running (meaning we have previously
			// encountered) a running stage OR the current stage is running, then our
			// job status should be set to running.
			m{
				"case": m{"$eq": arr{"$$value.status", lucy.Status_RUNNING}},
				"then": "$$value.status",
			},
			m{
				"case": m{"$eq": arr{"$$this.status", lucy.Status_RUNNING}},
				"then": "$$this.status",
			},

			// For a job to be completed, then ALL stages must be completed. We start
			// our $$value with status = completed, and as long as it hasn't been
			// changed by any previous stages AND the current stage is also completed,
			// we keep completed.
			m{
				"case": m{
					"$and": arr{
						m{"$eq": arr{"$$this.status", lucy.Status_COMPLETED}},
						m{"$eq": arr{"$$value.status", lucy.Status_COMPLETED}},
					},
				},
				"then": "$$value.status",
			},
		},

		// Otherwise we have encountered a pending stage, and all previous stages have
		// also been pending, so we are pending.
		"default": lucy.Status_PENDING,
	},
}

// updateStageResultSwitch is a switch expression used in updateJobSummaries to
// update the stage.result field.
var updateStageResultSwitch = m{
	"$switch": m{
		"branches": arr{
			m{
				"case": m{"$eq": arr{"$$value.result", lucy.Result_FAILED}},
				"then": "$$value.result",
			},
			m{
				"case": m{"$eq": arr{"$$this.result", lucy.Result_FAILED}},
				"then": "$$this.result",
			},
			m{
				"case": m{
					"$and": arr{
						m{"$eq": arr{"$$this.result", lucy.Result_SUCCEEDED}},
						m{"$eq": arr{"$$value.result", lucy.Result_SUCCEEDED}},
					},
				},
				"then": "$$value.result",
			},
		},
		"default": lucy.Result_NONE,
	},
}

// UpdateBatchSummaries is a bson expression to summarize the batch status and result
// counts.
var UpdateBatchSummaries = m{
	"$replaceRoot": m{
		"newRoot": m{
			"$mergeObjects": arr{
				"$$ROOT",
				m{"job_count": m{"$size": "$jobs"}},
				m{
					"$reduce": m{
						"input": "$jobs",
						"initialValue": m{
							"pending_count":   uint32(0),
							"cancelled_count": uint32(0),
							"running_count":   uint32(0),
							"completed_count": uint32(0),
							"success_count":   uint32(0),
							"failure_count":   uint32(0),
							"run_count":       uint32(0),
							"progress": float32(0),
						},
						"in": m{
							"pending_count": bsonIncrementSummary(
								"$$value.pending_count",
								"$$this.status",
								lucy.Status_PENDING,
							),
							"cancelled_count": bsonIncrementSummary(
								"$$value.cancelled_count",
								"$$this.status",
								lucy.Status_CANCELLED,
							),
							"running_count": bsonIncrementSummary(
								"$$value.running_count",
								"$$this.status",
								lucy.Status_RUNNING,
							),
							"completed_count": bsonIncrementSummary(
								"$$value.completed_count",
								"$$this.status",
								lucy.Status_COMPLETED,
							),
							"success_count": bsonIncrementSummary(
								"$$value.success_count",
								"$$this.result",
								lucy.Result_SUCCEEDED,
							),
							"failure_count": bsonIncrementSummary(
								"$$value.failure_count",
								"$$this.result",
								lucy.Result_FAILED,
							),
							"progress": m{
								"$add": arr{"$$value.progress", "$$this.progress"},
							},
						},
					},
				},
			},
		},
	},
}

// FinalizeBatchProgressStage is an aggregation pipeline stage that calculates the total
// progress of the batch.
var FinalizeBatchProgressStage = m{
	// We want to handle floating point errors here, if all jobs are
	// complete then our progress is 1.0, otherwise it is the sum of
	// all job progress fields divided by job count. If that sum exceeds 1.0 due to
	// floating-point errors, then cap it at 1.0.
	"$set": m{
		"progress": m{
			"$cond": arr{
				m{"$eq": arr{"$complete_count", "$job_count"}},
				float32(1.0),
				"$progress",
			},
		},
	},
}
