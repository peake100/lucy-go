package lucysql

import (
	"context"
	"errors"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // import the sqlite driver so it is available.
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/pkg/lucy"
	"os"
	"path"
	"testing"
)

// Backend holds our connection information and methods for sqlLite and implements
// db.Backend.
type Backend struct {
	db       *sqlx.DB
	prepared statements
	errs     *pkerr.ErrorGenerator
}

func (backend Backend) setupTables(ctx context.Context) error {
	_, err := backend.db.ExecContext(ctx, sqlSchema)

	if err != nil {
		return fmt.Errorf("error declaring tables: %w", err)
	}

	return nil
}

func (backend *Backend) prePrepareStatements(ctx context.Context) (err error) {
	// Pre-prepare statements on the server.
	backend.prepared, err = newStatements(ctx, backend.db)
	return err
}

// Tester returns a testing harness for testing backend. This harness' methods should
// NOT be used in the production code.
func (backend Backend) Tester(t *testing.T) BackendTester {
	return BackendTester{
		t:       t,
		backend: backend,
	}
}

func (backend Backend) Disconnect(ctx context.Context) error {
	return backend.db.Close()
}

func (backend Backend) GetBatchJobs(
	ctx context.Context, batchId *cereal.UUID,
) (db.ResultGetBatchJobs, error) {
	panic("implement me")
}

func (backend Backend) CancelBatches(
	ctx context.Context, batches *lucy.CancelBatches,
) (db.CancelBatchResultsCursor, error) {
	panic("implement me")
}

func (backend Backend) CancelJob(
	ctx context.Context, job *cereal.UUID,
) (db.ResultCancelJob, error) {
	panic("implement me")
}

func (backend Backend) UpdateStage(
	ctx context.Context, update db.StageUpdate,
) (db.ResultWorkerUpdate, error) {
	panic("implement me")
}

// BackendTester is a testing harness for use with Backend.
type BackendTester struct {
	t       *testing.T
	backend Backend
}

// DB returns the underlying db connection for manipulation during tests.
func (tester BackendTester) DB() *sqlx.DB {
	return tester.backend.db
}

// New creates a new sqlite Backend and returns it as a db.Backend interface.
func New(
	ctx context.Context, errGen *pkerr.ErrorGenerator,
) (db.Backend, error) {
	backend, err := newBackend(ctx, errGen)
	if err != nil {
		return nil, err
	}

	err = backend.setupTables(ctx)
	if err != nil {
		return nil, err
	}

	err = backend.prePrepareStatements(ctx)
	if err != nil {
		return nil, fmt.Errorf("error pre-prepariing statements on db: %w", err)
	}

	return backend, nil
}

func newBackend(
	ctx context.Context, errGen *pkerr.ErrorGenerator,
) (Backend, error) {
	filePath := os.Getenv(EnvKeySQLiteFile)
	if filePath == "" {
		return Backend{}, fmt.Errorf(
			"no SQLite filepath specified. please set '%v' env var with path"+
				" to the target database",
			EnvKeySQLiteFile,
		)
	}

	// Create the parent directory.
	dir := path.Dir(filePath)
	err := os.MkdirAll(dir, 0755)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return Backend{}, fmt.Errorf("error creating parent directory: %w", err)
	}

	// Create the file.
	file, err := os.OpenFile(
		filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755,
	)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return Backend{}, fmt.Errorf("error creating database file: %w", err)
	}
	_ = file.Close()

	sqlDB, err := sqlx.Connect("sqlite3", filePath)
	if err != nil {
		return Backend{}, fmt.Errorf("error opening database file: %w", err)
	}

	backend := Backend{
		db:       sqlDB,
		prepared: statements{},
		errs:     errGen,
	}

	return backend, nil
}
