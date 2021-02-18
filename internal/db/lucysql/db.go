package lucysql

import (
	"context"
	"database/sql"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
)

// Backend holds our connection information and methods for sqlLite and implements
// db.Backend.
type Backend struct {
	Client *sql.DB
}

func (b Backend) GetBatch(ctx context.Context, batchId *cerealMessages.UUID) (db.ResultGetBatch, error) {
	panic("implement me")
}

func (b Backend) ListBatches(ctx context.Context) (db.ListBatchesCursor, error) {
	panic("implement me")
}

func (b Backend) GetBatchJobs(ctx context.Context, batchId *cerealMessages.UUID) (db.ResultGetBatchJobs, error) {
	panic("implement me")
}

func (b Backend) CancelBatches(ctx context.Context, batches *lucy.CancelBatches) (db.CancelBatchResultsCursor, error) {
	panic("implement me")
}

func (b Backend) GetJob(ctx context.Context, jobId *cerealMessages.UUID) (db.ResultGetJob, error) {
	panic("implement me")
}

func (b Backend) CreateBatch(ctx context.Context, batch *lucy.NewBatch) (db.ResultCreateBatch, error) {
	panic("implement me")
}

func (b Backend) CreateJobs(ctx context.Context, jobs *lucy.NewJobs) (db.ResultCreateJobs, error) {
	panic("implement me")
}

func (b Backend) CancelJob(ctx context.Context, job *cerealMessages.UUID) (db.ResultCancelJob, error) {
	panic("implement me")
}

func (b Backend) UpdateStage(ctx context.Context, update db.StageUpdate) (db.ResultWorkerUpdate, error) {
	panic("implement me")
}

