package lucysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/illuscio-dev/protoCereal-go/protosql"
	"github.com/jmoiron/sqlx"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/internal/db/internal"
	"github.com/peake100/lucy-go/pkg/lucy"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (backend Backend) CreateBatch(
	ctx context.Context, batch *lucy.NewBatch,
) (db.ResultCreateBatch, error) {
	// Create a new context so we can cancel the transaction immediately on an error.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	recordId, err := internal.NewRecordId()
	if err != nil {
		return db.ResultCreateBatch{}, fmt.Errorf(
			"error creating record id: %w", err,
		)
	}

	recordIdBytes := recordId.MustGoogle()

	// We'll use this struct to insert our new data.
	type Insert struct {
		Id          []byte `db:"id"`
		Name        string `db:"name"`
		Type        string `db:"type"`
		Description string `db:"description"`
	}

	_, err = backend.prepared.InsertBatch.ExecContext(
		ctx,
		Insert{
			Id:          recordIdBytes[:],
			Name:        batch.Name,
			Type:        batch.Type,
			Description: batch.Description,
		},
	)
	if err != nil {
		return db.ResultCreateBatch{}, fmt.Errorf(
			"error inserting record: %w", err,
		)
	}

	// And this struct to retrieve it.
	type Result struct {
		Created  time.Time `db:"created"`
		Modified time.Time `db:"modified"`
	}

	row := backend.db.QueryRowxContext(
		ctx,
		`SELECT created, modified FROM jobs_batches WHERE id=?`,
		recordIdBytes[:],
	)
	if row.Err() != nil {
		return db.ResultCreateBatch{}, fmt.Errorf(
			"error querying record: %w", row.Err(),
		)
	}

	dbResult := Result{}
	err = row.StructScan(&dbResult)
	if err != nil {
		return db.ResultCreateBatch{}, fmt.Errorf(
			"error decoding query result: %w", err,
		)
	}

	return db.ResultCreateBatch{
		BatchId:  recordId,
		Created:  timestamppb.New(dbResult.Created),
		Modified: timestamppb.New(dbResult.Modified),
	}, nil
}

func (backend Backend) CreateJobs(
	ctx context.Context, jobs *lucy.NewJobs,
) (db.ResultCreateJobs, error) {
	result := db.ResultCreateJobs{
		Jobs:           &lucy.CreatedJobs{Ids: make([]*cereal.UUID, len(jobs.Jobs))},
		BatchSummaries: db.ResultBatchSummaries{},
	}

	// We'll use this struct to extract that batch summary info at the end.
	batchSummariesRecord := struct {
		*db.ResultBatchSummaries
		Modified protosql.TimestampMarshaller `db:"modified"`
	}{
		// We'll use a pointer to the batch summaries on our result object to unmarshal
		// the data.
		ResultBatchSummaries: &result.BatchSummaries,
		Modified:             protosql.TimestampMarshaller{},
	}

	records, err := createJobsPrepRecords(jobs)
	if err != nil {
		return db.ResultCreateJobs{}, fmt.Errorf(
			"error prepping job records: %w", err,
		)
	}

	// Begin a transaction.
	tx, err := backend.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return db.ResultCreateJobs{}, fmt.Errorf(
			"error starting transaction: %w", err,
		)
	}

	// Prep our statements specific to this transaction.
	sqlStatements := createJobsStatements{
		InsertJob:   tx.NamedStmtContext(ctx, backend.prepared.InsertJob),
		InsertStage: tx.NamedStmtContext(ctx, backend.prepared.InsertStage),
		GetBatch:    tx.StmtxContext(ctx, backend.prepared.GetBatchSummaries),
	}

	// Iterate over our jobs records and create them.
	for i, thisRecord := range records {
		err = sqlStatements.insertJob(ctx, thisRecord)
		if err != nil {
			return result, fmt.Errorf("error creating job %v: %w", i, err)
		}
		result.Jobs.Ids[i] = thisRecord.Job.Id
	}

	err = sqlStatements.GetBatch.GetContext(ctx, &batchSummariesRecord, jobs.Batch)
	if err != nil {
		return result, fmt.Errorf("error fetching batch summaries: %w", err)
	}

	// Commit the transaction.
	err = tx.Commit()
	if err != nil {
		return result, fmt.Errorf("error commiting transations")
	}

	// Extract the modified time.
	result.BatchSummaries.Modified = batchSummariesRecord.Modified.Timestamp

	return result, nil
}

type createJobRecord struct {
	Job    insertJobRecord
	Stages []insertStageRecord
}

type insertJobRecord struct {
	*lucy.NewJob
	Id      *cereal.UUID                    `db:"id"`
	BatchId *cereal.UUID                    `db:"batch_id"`
	Input   *protosql.MessageBlobMarshaller `db:"input"`
}

type insertStageRecord struct {
	*lucy.JobStage
	StageIndex int64        `db:"stage_index"`
	JobId      *cereal.UUID `db:"job_id"`
}

// createJobsPrepRecords preps all of our insertion records ahead of time so the
// transaction can be as short as possible.
func createJobsPrepRecords(jobs *lucy.NewJobs) ([]createJobRecord, error) {
	records := make([]createJobRecord, len(jobs.Jobs))

	for i, newJob := range jobs.Jobs {
		jobId, err := internal.NewRecordId()
		if err != nil {
			return nil, fmt.Errorf("error creating job records")
		}

		record := createJobRecord{
			Job: insertJobRecord{
				NewJob:  newJob,
				Id:      jobId,
				BatchId: jobs.Batch,
				Input:   protosql.Message(newJob.Input),
			},
			Stages: make([]insertStageRecord, len(newJob.Stages)),
		}

		for j, newStage := range newJob.Stages {
			record.Stages[j] = insertStageRecord{
				JobStage:   newStage,
				StageIndex: int64(j),
				JobId:      jobId,
			}
		}

		records[i] = record
	}

	return records, nil
}

// createJobsStatements holds prepared statements tied to a specific transaction for
// creating records.
type createJobsStatements struct {
	InsertJob   *sqlx.NamedStmt
	InsertStage *sqlx.NamedStmt
	GetBatch    *sqlx.Stmt
}

func (createJobs createJobsStatements) insertJob(
	ctx context.Context, record createJobRecord,
) error {
	// Insert the job.
	_, err := createJobs.InsertJob.ExecContext(ctx, record.Job)
	if err != nil {
		return fmt.Errorf("error inserting job: %w", err)
	}

	for _, thisRecord := range record.Stages {
		_, err = createJobs.InsertStage.ExecContext(ctx, thisRecord)
		if err != nil {
			return fmt.Errorf("error inserting stages: %w", err)
		}
	}

	return nil
}
