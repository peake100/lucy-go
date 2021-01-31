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

func BsonSetCurrentDates(update bson.M, fields []string) bson.M {
	fieldUpdates := make(bson.M, len(fields))
	for _, thisField := range fields {
		fieldUpdates[thisField] = "$$NOW"
	}

	update["$set"] = fieldUpdates
	return update
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
	// extract the job into a temporary field while we update it
	extractJob := m{
		"$set": m{
			TempJobField: m{
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

	// extract the stage
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

	dateFields := []string{
		"modified",
		TempJobField + ".modified",
		TempStageField + ".modified",
	}
	// If there is a stage time field like 'started' or 'completed' to update, do so
	// here.
	if timeField != "" {
		dateFields = append(dateFields, TempStageField + "." + timeField)
	}

	// Update modified field on the batch, job, and stage.
	currentDateUpdates := BsonSetCurrentDates(
		bson.M{},
		dateFields,
	)

	// update the job stage
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

	// insert stage in job
	insertStage := m{
		"$set": m{
			TempJobField + ".stages": BsonUpdateArrayAtIndex(
				"$"+TempJobField+".stages",
				int(stageId.StageIndex),
				"$"+TempStageField,
			),
		},
	}

	insertJob := m{
		"$set": m{
			"jobs": BsonUpdateArrayItemOnMatch(
				"$jobs",
				m{"$eq": arr{"$$this.id", stageId.JobId}},
				"$"+TempJobField,
			),
		},
	}

	removeTempFields := m{
		"$unset": arr{TempJobField, TempStageField},
	}

	return arr{
		extractJob,
		extractStage,
		currentDateUpdates,
		updateStage,
		insertStage,
		updateJobSummaries,
		insertJob,
		removeTempFields,
		UpdateBatchSummaries,
		FinalizeBatchProgressStage,
	}
}
