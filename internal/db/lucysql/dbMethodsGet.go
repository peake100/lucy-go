package lucysql

import (
	"context"
	"errors"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/illuscio-dev/protoCereal-go/protosql"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"google.golang.org/protobuf/types/known/anypb"
)

func (backend Backend) GetBatch(
	ctx context.Context, batchId *cereal.UUID,
) (db.ResultGetBatch, error) {
	rows, err := backend.prepared.GetBatch.QueryxContext(
		ctx,
		batchId,
	)
	if err != nil {
		return db.ResultGetBatch{}, fmt.Errorf("error fetching batch recordd: %w", err)
	}
	if !rows.Next() {
		return db.ResultGetBatch{}, pkerr.ErrNotFound
	}
	batchRecord := struct {
		// Embed the summaries.
		*lucy.Batch
		// We can't unmarshall timestamps directly so we need to use a marshaller here.
		Created  protosql.TimestampMarshaller `db:"created"`
		Modified protosql.TimestampMarshaller `db:"modified"`
	}{}
	err = rows.StructScan(&batchRecord)
	if err != nil {
		return db.ResultGetBatch{}, fmt.Errorf(
			"error decoing batch record: %w", err,
		)
	}
	// Extract the timestamp protos.
	batchRecord.Batch.Created = batchRecord.Modified.Timestamp
	batchRecord.Batch.Modified = batchRecord.Modified.Timestamp

	// return the result
	result := db.ResultGetBatch{
		Batch: batchRecord.Batch,
	}

	return result, nil
}

func (backend Backend) ListBatches(ctx context.Context) (db.ListBatchesCursor, error) {
	panic("implement me")
}

func (backend Backend) GetJob(
	ctx context.Context, jobId *cereal.UUID,
) (db.ResultGetJob, error) {
	jobRecord := struct {
		*lucy.Job
		Created  protosql.TimestampMarshaller   `db:"created"`
		Modified protosql.TimestampMarshaller   `db:"modified"`
		Input    protosql.MessageBlobMarshaller `db:"input"`
		Status   protosql.EnumStringMarshaller  `db:"status"`
		Result   protosql.EnumStringMarshaller  `db:"result"`
	}{
		Job:      new(lucy.Job),
		Created:  protosql.TimestampMarshaller{},
		Modified: protosql.TimestampMarshaller{},
		Input: protosql.MessageBlobMarshaller{
			Message: new(anypb.Any),
		},
		Status: protosql.EnumStringMarshaller{
			Enum: lucy.Status_PENDING,
		},
		Result: protosql.EnumStringMarshaller{
			Enum: lucy.Result_NONE,
		},
	}

	type StageRecord struct {
		*lucy.JobStage
		Started    protosql.TimestampMarshaller   `db:"started"`
		Completed  protosql.TimestampMarshaller   `db:"completed"`
		Status     protosql.EnumStringMarshaller  `db:"status"`
		Result     protosql.EnumStringMarshaller  `db:"result"`
		Error      protosql.MessageBlobMarshaller `db:"error"`
		ResultData protosql.MessageBlobMarshaller `db:"result_data"`
	}

	err := backend.prepared.GetJob.GetContext(ctx, &jobRecord, jobId)
	if err != nil {
		return db.ResultGetJob{}, fmt.Errorf(
			"error getting job records: %w", err,
		)
	}

	rows, err := backend.prepared.GetJobStages.QueryxContext(ctx, jobId)
	if err != nil {
		return db.ResultGetJob{}, fmt.Errorf(
			"error getting job stage records: %w", err,
		)
	}

	i := -1
	for rows.Next() {
		i++
		thisRecord := StageRecord{
			JobStage:  new(lucy.JobStage),
			Started:   protosql.TimestampMarshaller{},
			Completed: protosql.TimestampMarshaller{},
			Status: protosql.EnumStringMarshaller{
				Enum: lucy.Status_PENDING,
			},
			Result: protosql.EnumStringMarshaller{
				Enum: lucy.Result_NONE,
			},
			Error: protosql.MessageBlobMarshaller{
				Message: new(pkerr.Error),
			},
			ResultData: protosql.MessageBlobMarshaller{
				Message: new(anypb.Any),
			},
		}
		err = rows.StructScan(&thisRecord)
		if err != nil {
			return db.ResultGetJob{}, fmt.Errorf(
				"error decoding stage %v: %w", i, err,
			)
		}
		stageProto := thisRecord.JobStage

		stageProto.Status = thisRecord.Status.Enum.(lucy.Status)
		stageProto.Result = thisRecord.Result.Enum.(lucy.Result)
		stageProto.Started = thisRecord.Started.Timestamp
		stageProto.Completed = thisRecord.Completed.Timestamp
		if thisRecord.ResultData.Message != nil {
			stageProto.ResultData = thisRecord.ResultData.Message.(*anypb.Any)
		}
		if thisRecord.Error.Message != nil {
			stageProto.Error = thisRecord.Error.Message.(*pkerr.Error)
		}
		jobRecord.Job.Stages = append(jobRecord.Job.Stages, stageProto)
	}

	// Extract timestamp fields.
	jobRecord.Job.Created = jobRecord.Created.Timestamp
	jobRecord.Job.Modified = jobRecord.Modified.Timestamp
	input, ok := jobRecord.Input.Message.(*anypb.Any)
	if !ok {
		return db.ResultGetJob{}, errors.New(
			"input data did not unmarshal to *anypb.Any",
		)
	}
	jobRecord.Job.Input = input

	result := db.ResultGetJob{
		Job: jobRecord.Job,
	}

	return result, nil
}
