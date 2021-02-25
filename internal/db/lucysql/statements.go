package lucysql

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type statements struct {
	GetBatch     *sqlx.Stmt
	GetJob       *sqlx.Stmt
	GetJobStages *sqlx.Stmt

	InsertBatch *sqlx.NamedStmt
	InsertJob   *sqlx.NamedStmt
	InsertStage *sqlx.NamedStmt

	GetBatchSummaries *sqlx.Stmt
}

func newStatements(ctx context.Context, db *sqlx.DB) (prepared statements, err error) {
	prepared.GetBatch, err = db.PreparexContext(
		ctx,
		`SELECT * FROM jobs_batches WHERE id == ? LIMIT 1`,
	)
	if err != nil {
		return statements{}, fmt.Errorf("error preparing get batch: %w", err)
	}

	prepared.GetJob, err = db.PreparexContext(
		ctx,
		`SELECT
			id,
       		created,
       		modified,
       		type,
       		name,
       		description,
       		input,
       		run_count,
       		max_retries,
       		progress,
       		status,
       		result
		FROM jobs_jobs WHERE id == ? LIMIT 1`,
	)
	if err != nil {
		return statements{}, fmt.Errorf("error preparing get job: %w", err)
	}

	prepared.GetJobStages, err = db.PreparexContext(
		ctx,
		`SELECT
			type,
			description,
       		status,
       		run_by,
       		started,
       		completed,
       		progress,
       		result,
       		error,
       		result_data,
       		run_count
		FROM jobs_stages WHERE job_id == ? ORDER BY stage_index`,
	)
	if err != nil {
		return statements{}, fmt.Errorf(
			"error preparing get job stages: %w", err,
		)
	}

	prepared.InsertBatch, err = db.PrepareNamedContext(
		ctx,
		`INSERT INTO jobs_batches (id, name, type, description)
		VALUES (:id, :name, :type, :description)`,
	)
	if err != nil {
		return prepared, fmt.Errorf("error preparing insert batch: %w", err)
	}

	prepared.InsertJob, err = db.PrepareNamedContext(
		ctx,
		`INSERT INTO jobs_jobs (
		   id, 
		   batch_id, 
		   type, 
		   name,
		   description, 
		   input, 
		   max_retries
		)
		VALUES (:id, :batch_id, :type, :name, :description, :input, :max_retries)`,
	)
	if err != nil {
		return prepared, fmt.Errorf("error preparing insert job: %w", err)
	}

	prepared.InsertStage, err = db.PrepareNamedContext(
		ctx,
		`INSERT INTO jobs_stages (job_id, stage_index, type, description)
		VALUES (:job_id, :stage_index, :type, :description)`,
	)
	if err != nil {
		return prepared, fmt.Errorf("error preparing insert stage: %w", err)
	}

	prepared.GetBatchSummaries, err = db.PreparexContext(
		ctx,
		`SELECT
			id, 
			modified, 
			progress,
			job_count,
			pending_count,
			cancelled_count,
			running_count,
			completed_count,
			success_count,
			failure_count
		FROM jobs_batches 
		WHERE id==?
		LIMIT 1`,
	)
	if err != nil {
		return prepared, fmt.Errorf(
			"error preparing get batch summaries: %w", err,
		)
	}

	return prepared, nil
}
