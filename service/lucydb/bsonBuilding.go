package lucydb

import (
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/lucy-go/lucy"
	"go.mongodb.org/mongo-driver/bson"
)

// NewRecordId creates a uuid and creation time for inserting new records.
func NewRecordId() (recordId *cerealMessages.UUID, err error) {
	recordId, err = cerealMessages.NewUUIDRandom()
	if err != nil {
		return nil, fmt.Errorf(
			"error generating new UUID: %w", err,
		)
	}

	return recordId, nil
}

// BsonUpdateArrayAtIndex creates a $map expression that updates array at index with
// value.
func BsonUpdateArrayAtIndex(array string, index interface{}, value interface{}) m {
	return m{
		"$map": m{
			"input": m{"$range": arr{0, m{"$size": array}}},
			"in": m{
				"$cond": arr{
					m{"$eq": arr{"$$this", index}},
					value,
					m{"$arrayElemAt": arr{array, "$$this"}},
				},
			},
		},
	}
}

// BsonUpdateArrayItemOnMatch creates a $map expression that replaces an item in array
// with value if filterExpr returns true.
//
// filterExpr can use $$this to inspect each item.
func BsonUpdateArrayItemOnMatch(
	array string, filterExpr bson.M, value interface{},
) bson.M {
	return m{
		"$map": m{
			"input": array,
			"in": m{
				"$cond": arr{
					filterExpr,
					value,
					"$$this",
				},
			},
		},
	}
}

// BsonStageSetCurrentDates takes a list of fields and applies the current datetime to
// them.
func BsonStageSetCurrentDates(update bson.M, fields []string) bson.M {
	fieldUpdates := make(bson.M, len(fields))
	for _, thisField := range fields {
		fieldUpdates[thisField] = "$$NOW"
	}

	update["$set"] = fieldUpdates
	return update
}

// MustCompileStaticDocument takes a bson.M and pre-compiles it using our registry to a
// bson.Raw. This moves the overhead of serialization for large static query objects
// from per-request runtime to once-at-initialization.
func MustCompileStaticDocument(val bson.M) bson.Raw {
	bin, err := bson.MarshalWithRegistry(BsonRegistry, val)
	if err != nil {
		panic(fmt.Errorf("error precompiling bson: %w", err))
	}

	return bin
}

// MustCompileStaticPipeline takes a bson.A value and pre-marshals it's items into s
// []bson.Raw value to reduce marshalling load on each request.
func MustCompileStaticPipeline(val bson.A) []bson.Raw {
	result := make([]bson.Raw, len(val))
	for i, thisDocument := range val {
		bin, err := bson.MarshalWithRegistry(BsonRegistry, thisDocument)
		if err != nil {
			panic(fmt.Errorf("error precompiling pipeline: %w", err))
		}
		result[i] = bin
	}

	return result
}

// bsonIncrementSummary will increment the summary field in a reduce expression if
// the field equals ifEquals.
func bsonIncrementSummary(
	summaryField string,
	valueField string,
	ifEquals interface{},
) bson.M {
	return bson.M{
		"$cond": arr{
			// if
			m{"$eq": arr{valueField, ifEquals}},
			// then
			m{"$add": arr{summaryField, 1}},
			summaryField,
		},
	}
}

// CreateStageUpdatePipeline creates the bson array pipeline value to update a given
// stage of a given job in a batch record.
func CreateStageUpdatePipeline(
	stageId *lucy.StageID, updateObject interface{}, timeField string,
) (pipeline bson.A) {
	// SEE THE BOTTOM OF THIS FUNCTION FOR A FULL EXPLANATION OF THE UPDATE PIPELINE
	// FLOW.

	// extracts the job into a temporary field for modification. Being able to
	// manipulate the job as a top-level field will make the calculations in subsequent
	// pipeline stages much easier to reason about and craft.
	//
	// Pipelines are atomic, so no other caller will ever observe these fields existing.
	extractJob := m{
		"$set": m{
			TempJobField: m{
				// Get the element at the array index of the value that matches our
				// job id.
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
	}

	// extracts the stage into a temporary field for modification.
	extractStage := m{
		"$set": m{
			TempStageField: m{
				"$arrayElemAt": arr{
					"$" + TempJobField + ".stages",
					stageId.StageIndex,
				},
			},
		},
	}

	// Get the list of date fields we need to put the current timestamp in.s
	dateFields := []string{
		"modified",
		TempJobField + ".modified",
		TempStageField + ".modified",
	}
	// If there is a stage time field like 'started' or 'completed' to update, do so
	// here.
	if timeField != "" {
		dateFields = append(dateFields, TempStageField+"."+timeField)
	}

	// Updates modified fields on the batch, job, and stage.
	currentDateUpdates := BsonStageSetCurrentDates(
		bson.M{},
		dateFields,
	)

	// Merges our stage update object into the job stage to update it with the
	// caller-passed data.
	updateStage := m{
		"$set": m{
			TempStageField: m{
				"$mergeObjects": arr{
					"$" + TempStageField,
					updateObject,
				},
			},
		},
	}

	// re-inserts temp stage into job, overwriting the unmodified value.
	insertStage := m{
		"$set": m{
			TempJobField + ".stages": BsonUpdateArrayAtIndex(
				"$"+TempJobField+".stages",
				int(stageId.StageIndex),
				"$"+TempStageField,
			),
		},
	}

	// re-inserts the job into the batch, overwriting the unmodified value.
	insertJob := m{
		"$set": m{
			"jobs": BsonUpdateArrayItemOnMatch(
				"$jobs",
				m{"$eq": arr{"$$this.id", stageId.JobId}},
				"$"+TempJobField,
			),
		},
	}

	// Let's go over the flow of this pipeline:
	return arr{
		// 1. First we extract the job we want into a top-level field called
		//    "job_to_update". This will allow us to operate on the job more easily.
		//    We MIGHT be able to do this with a bunch of nested $let operators, but
		//    this will be less mental overhead for crafting subsequent updates.
		extractJob,
		// 2. Next we do the same thing to the job stage, extracting it from our temp
		//    job and putting it's own field.
		extractStage,
		// 3. Here we update all fields that need the same matching current datetime:
		//    modified fields, stage.started, etc.
		currentDateUpdates,
		// 4. Now we update our stage, which is a bit easier to do now that it is
		//    extracted.
		updateStage,
		// 5. Re-insert our updated stage into the stages field of our extracted job,
		//    overwriting the original value.
		insertStage,
		// 6. Re-calculate any summaries fields on the job that could be affected by the
		//    new stage data.
		stageUpdatePipelineUpdateJobSummaries,
		// 7. The previous stage sums the progress of the stages, we still need to
		//    average and clamp it.
		stageUpdatePipelineFinalizeJobProgress,
		// 8. Re-insert the job into the batch.jobs array, overwriting it's original
		//    value.
		insertJob,
		// 9. Re-calculate the batch summary fields that might have been affected by the
		//    updated job information.
		UpdateBatchSummaries,
		// 10. The previous step sums all of the job.progress fields, we still need to
		//     average and clamp the value here.
		FinalizeBatchProgressStage,
		// 11. Remove the temporary fields we were storing our job and job stage on for
		//     updates.
		stageUpdatePipelineRemoveTempFields,
	}
}

// UpdateJobSummaries updates the a job's summaries where the job is identified by
// jobIdentifier (field name or var).
func UpdateJobSummaries(jobIdentifier string) bson.M {
	return m{
		// We're going to reduce on the job.stages field to build a temporary object
		// with 'result' and 'status' fields that we will then merge into our job
		// document to update it.
		"$mergeObjects": arr{
			jobIdentifier,
			m{
				"$reduce": m{
					"input": jobIdentifier + ".stages",
					"initialValue": m{
						// Start with status = COMPLETED and result = SUCCEEDED.
						// See comments on updateStageStatusSwitch and
						// updateStageResultSwitch for an explanation of this
						// starting state.
						"status":    statusUnset,
						"result":    lucy.Result_SUCCEEDED,
						"run_count": 0,
						"progress":  0,
					},
					"in": updateJobSummariesReduceIn,
				},
			},
		},
	}
}

// finalizeJobProgress takes a summed job.progress field and averages / clamps it.
func finalizeJobProgress(jobIdentifier string) bson.M {
	return m{
		"$min": arr{
			float32(1.0),
			m{
				"$divide": arr{
					jobIdentifier + ".progress",
					m{"$size": jobIdentifier + ".stages"},
				},
			},
		},
	}
}

// CancelJobsPipeline creates the update pipeline to cancel jobs from a list oh job ids.
// Only PENDING jobs will be cancelled.
//
// If jobIds is nil, all pending jobs in affected batches will be cancelled.
func CancelJobsPipeline(jobId *cerealMessages.UUID) bson.A {
	// We are going to do this in a single map action.
	mapAction := m{
		"$let": m{
			"vars": m{
				"summarized": m{
					"$let": m{
						// First we cancel our pending stages and store that in a
						// $$cancelled var.
						"vars": m{
							"cancelled": mapStageCancellation,
						},
						// Then we update the job summaries. This in turn gets set to
						// the $$summarized var.
						"in": UpdateJobSummaries(
							"$$cancelled",
						),
					},
				},
			},
			// Now we finalize the summaries from the job held in the $$summarized
			// var.
			"in": bson.M{
				"$mergeObjects": arr{
					"$$summarized",
					m{
						"progress": finalizeJobProgress("$$summarized"),
						// Update the modified field.
						"modified": "$$NOW",
					},
				},
			},
		},
	}

	// If we have a job id pass-list, put our update behind a check to see if the job
	// id is in the list.
	if jobId != nil {
		mapAction = m{
			"$cond": arr{
				m{"$eq": arr{"$$job.id", jobId}},
				mapAction,
				"$$job",
			},
		}
	}

	// Now build the map computation.
	updateStageStatuses := m{
		"$set": m{
			"jobs": m{
				"$map": m{
					"input": "$jobs",
					"as":    "job",
					"in":    mapAction,
				},
			},
			// Update the modified field.
			"modified": "$$NOW",
		},
	}

	return arr{
		// Update the stage statuses to CANCELLED.
		updateStageStatuses,
		// Once the jon updates are done we need to re-summarize the batch
		// (cancelled_count will be changed).
		UpdateBatchSummaries,
		// Updating the summaries means we need to average and clamp the progress.
		FinalizeBatchProgressStage,
	}
}
