package main_test

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pktesting"
	"github.com/peake100/lucy-go/lucy"
	"github.com/peake100/lucy-go/service"
	"github.com/peake100/lucy-go/service/lucydb"
	"github.com/peake100/lucy-go/service/prototesting"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"os"
	"sync"
	"testing"
	"time"
)

func init() {
	os.Setenv("LUCY_MONGO_URI", "mongodb://127.0.0.1:57017")
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

const BatchType = "SortStudents"

// This suite is going to test running batches.
type LucyBatchUnarySuite struct {
	pktesting.ManagerSuite

	db     lucydb.LucyDB
	client lucy.LucyClient

	batchId           *cerealMessages.UUID
	batchAdded        *lucy.NewBatch
	lastBatchModified time.Time

	jobId    *cerealMessages.UUID
	jobAdded *lucy.NewJob
}

func (suite *LucyBatchUnarySuite) SetupSuite() {
	// get the db connector
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	var err error
	suite.db, err = lucydb.Connect(ctx)
	if !suite.NoError(err, "connect to db") {
		suite.FailNow("could not connect to db")
	}

	// Drop the lucy database
	err = suite.db.DB.Drop(ctx)
	if !suite.NoError(err, "drop database") {
		suite.FailNow("could not drop database")
	}

	manger, _ := service.NewLucyManager()
	suite.Manager = manger
	suite.ManagerSuite.SetupSuite()

	clientConn := suite.Manager.Test(suite.T()).GrpcClientConn(
		true, grpc.WithInsecure(),
	)

	suite.client = lucy.NewLucyClient(clientConn)
}

func (suite *LucyBatchUnarySuite) Test0010_NewBatch() {
	batch := &lucy.NewBatch{
		Type:        BatchType,
		Name:        "Sort Students",
		Description: "send new students through the sorting hat",
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	batchId, err := suite.client.CreateBatch(ctx, batch)
	if !suite.NoError(err, "new batch id") {
		suite.FailNow("could not create batch")
	}

	suite.T().Log("NEW BATCH ID:", batchId.MustGoogle())

	suite.batchId = batchId
	suite.batchAdded = batch
}

func (suite *LucyBatchUnarySuite) getBatch() *lucy.Batch {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	batch, err := suite.client.GetBatch(ctx, suite.batchId)
	if !suite.NoError(err, "get batch") {
		suite.FailNow("could not get batch")
	}

	return batch
}

func (suite *LucyBatchUnarySuite) checkBatchBasic(received *lucy.Batch) {
	suite.Equal(suite.batchId.MustGoogle(), received.Id.MustGoogle(), "id")
	suite.Equal(BatchType, received.Type, "type")
	suite.Equal(suite.batchAdded.Name, received.Name, "name")
	suite.Equal(suite.batchAdded.Description, received.Description)

	suite.NotNil(received.Created, "created not nil")
	suite.NotNil(received.Modified, "modified not nil")
}

func (suite *LucyBatchUnarySuite) Test0020_GetBatch() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Equal(batch.Modified.AsTime()),
		"modified equals created",
	)

	suite.Equal(uint32(0), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(0), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.0), batch.Progress, "progress: 0.0")
}

// Add job to batch
func (suite *LucyBatchUnarySuite) Test0030_AddJob() {
	input, err := anypb.New(&prototesting.Student{Name: "Harry Potter"})
	if !suite.NoError(err, "pack input") {
		suite.FailNow("could not pack job input")
	}

	job := &lucy.NewJob{
		Type:        "SortStudent",
		Name:        "Sort Student",
		Description: "sort student into a house",
		Input:       input,
		MaxRetries:  5,
		Stages: []*lucy.JobStage{
			{
				Type:        "Sort",
				Description: "sort the student into a house",
			},
			{
				Type:        "Announce",
				Description: "announce the house the student has been sorted into",
			},
		},
	}

	req := &lucy.NewJobs{
		Batch: suite.batchId,
		Jobs:  []*lucy.NewJob{job},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	created, err := suite.client.CreateJobs(ctx, req)
	if !suite.NoError(err, "create job") {
		suite.FailNow("could not create job")
	}

	suite.T().Log("JOB IDs:", created.Ids)

	if !suite.Len(created.Ids, 1, "returned 1 id") {
		suite.FailNow("unexpected created job ids count")
	}

	suite.jobId = created.Ids[0]
	suite.jobAdded = job
}

func (suite *LucyBatchUnarySuite) Test0040_GetBatch_JobPending() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified equals created",
	)

	suite.Equal(uint32(1), batch.JobCount, "no jobs")
	suite.Equal(uint32(1), batch.PendingCount, "no pending")
	suite.Equal(uint32(0), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.0), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) checkJobBasic(received *lucy.Job) {
	suite.Equal(suite.jobId.MustGoogle(), received.Id.MustGoogle())
	suite.NotNil(received.Created, "created present")
	suite.NotNil(received.Modified, "modified present")

	suite.Equal(suite.jobAdded.Type, received.Type, "type")
	suite.Equal(suite.jobAdded.Name, received.Name, "name")
	suite.Equal(
		suite.jobAdded.Description, received.Description, "description",
	)
	suite.Equal(
		suite.jobAdded.MaxRetries, received.MaxRetries, "max retries",
	)

	msg, err := received.Input.UnmarshalNew()
	if !suite.NoError(err, "unmarshall job info") {
		suite.FailNow("could not unmarshall job info")
	}

	student, ok := msg.(*prototesting.Student)
	if !ok {
		suite.T().Error("input is not student")
		suite.FailNow("bad job input data")
	}

	suite.Equal("Harry Potter", student.Name, "input data")

	if !suite.Len(received.Stages, 2, "1 job stage") {
		suite.FailNow("job stage not added")
	}

	stage1 := received.Stages[0]
	suite.Equal("Sort", stage1.Type, "stage 1 type")

	stage2 := received.Stages[1]
	suite.Equal("Announce", stage2.Type, "stage 2 type")
}

func (suite *LucyBatchUnarySuite) checkStagePending(stage *lucy.JobStage) {
	suite.Nil(stage.Started, "started nil")
	suite.Nil(stage.Completed, "completed nil")
	suite.Nil(stage.ResultData, "result data nil")
	suite.Nil(stage.Error, "error nil")
	suite.Zero(stage.RunBy, "no RunBy")
	suite.Equal(lucy.Status_PENDING, stage.Status)
	suite.Equal(lucy.Result_NONE, stage.Result)
}

func (suite *LucyBatchUnarySuite) checkStageRunning(stage *lucy.JobStage) {
	suite.NotNil(stage.Started, "started not nil")
	suite.Nil(stage.Completed, "completed nil")
	suite.Nil(stage.ResultData, "result data nil")
	suite.Nil(stage.Error, "error nil")
	suite.Equal("testworker", stage.RunBy, "runner")
	suite.Equal(lucy.Status_RUNNING, stage.Status, "status")
	suite.Equal(lucy.Result_NONE, stage.Result, "result")
}

func (suite *LucyBatchUnarySuite) getJob() *lucy.Job {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	job, err := suite.client.GetJob(ctx, suite.jobId)
	if !suite.NoError(err, "get job") {
		suite.FailNow("could not get job")
	}
	return job
}

func (suite *LucyBatchUnarySuite) Test0050_GetJob() {
	job := suite.getJob()

	suite.checkJobBasic(job)
	suite.True(
		job.Modified.AsTime().Equal(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_PENDING, job.Status, "status")
	suite.Equal(lucy.Result_NONE, job.Result, "result")
	suite.Equal(float32(0.0), job.Progress, "progress")
	suite.Equal(uint32(0), job.RunCount, "run count")

	for _, stage := range job.Stages {
		suite.checkStagePending(stage)
	}
}

func (suite *LucyBatchUnarySuite) Test0060_StartStage() {
	start := &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.StartStageUpdate{
			RunBy: "testworker",
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.StartStage(ctx, start)
	if !suite.NoError(err, "start stage") {
		suite.FailNow("failed to start stage 0")
	}
}

func (suite *LucyBatchUnarySuite) Test0070_GetJob_Started() {
	job := suite.getJob()

	suite.checkJobBasic(job)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_RUNNING, job.Status, "status")
	suite.Equal(lucy.Result_NONE, job.Result, "result")
	suite.Equal(float32(0.0), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.checkStageRunning(job.Stages[0])
	// Job modified time is completed time.
	suite.True(
		job.Modified.AsTime().Equal(job.Stages[0].Started.AsTime()),
		"started equals modified",
	)

	suite.checkStagePending(job.Stages[1])
}

func (suite *LucyBatchUnarySuite) Test0080_GetBatch_Started() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified before created",
	)

	suite.Equal(uint32(1), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(1), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.0), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0090_ProgressStage() {
	update := &lucy.ProgressStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.ProgressStageUpdate{Progress: 0.5},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.ProgressStage(ctx, update)
	if !suite.NoError(err, "update progress") {
		suite.FailNow("could not update progress")
	}
}

func (suite *LucyBatchUnarySuite) Test0100_GetJob_Progressing() {
	job := suite.getJob()

	suite.checkJobBasic(job)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_RUNNING, job.Status, "status")
	suite.Equal(lucy.Result_NONE, job.Result, "result")
	suite.Equal(float32(0.25), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.checkStageRunning(job.Stages[0])
	suite.Equal(float32(0.5), job.Stages[0].Progress, "progress")

	suite.checkStagePending(job.Stages[1])
}

func (suite *LucyBatchUnarySuite) Test0110_GetBatch_Progressing() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified equals created",
	)

	suite.Equal(uint32(1), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(1), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.25), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0120_CompleteStage_Success() {
	resultData, err := anypb.New(
		&prototesting.Sorted{House: prototesting.House_Gryffindor},
	)
	if !suite.NoError(err, "pack result") {
		suite.FailNow("failed to complete stage")
	}

	update := &lucy.CompleteStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.CompleteStageUpdate{
			Error:      nil,
			ResultData: resultData,
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err = suite.client.CompleteStage(ctx, update)
	if !suite.NoError(err, "update progress") {
		suite.FailNow("could not update progress")
	}
}

func (suite *LucyBatchUnarySuite) checkStageCompleted(stage *lucy.JobStage) {
	suite.NotNil(stage.Started, "started not nil")
	suite.NotNil(stage.Completed, "completed not nil")
	suite.NotNil(stage.ResultData, "result data nil")
	suite.Nil(stage.Error, "error nil")
	suite.Equal(lucy.Status_COMPLETED, stage.Status)
	suite.Equal(lucy.Result_SUCCEEDED, stage.Result)
}

func (suite *LucyBatchUnarySuite) Test0130_GetJob_OneCompleted() {
	job := suite.getJob()

	suite.checkJobBasic(job)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_RUNNING, job.Status, "status")
	suite.Equal(lucy.Result_NONE, job.Result, "result")
	suite.Equal(float32(0.5), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.checkStageCompleted(job.Stages[0])
	suite.Equal(float32(1.0), job.Stages[0].Progress, "progress")

	// Job modified time is completed time.
	suite.True(
		job.Modified.AsTime().Equal(job.Stages[0].Completed.AsTime()),
		"completed equals modified",
	)

	resultData, err := job.Stages[0].ResultData.UnmarshalNew()
	if !suite.NoError(err, "unmarshall result data") {
		suite.FailNow("could not get stage 0 result data")
	}

	sorted, ok := resultData.(*prototesting.Sorted)
	if !ok {
		suite.FailNow("stage 0 result data is not Sorted value")
	}

	suite.Equal(prototesting.House_Gryffindor, sorted.House, "result data")

	suite.checkStagePending(job.Stages[1])
}

func (suite *LucyBatchUnarySuite) Test0140_StartStage2() {
	start := &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 1,
		},
		Update: &lucy.StartStageUpdate{
			RunBy: "testworker",
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.StartStage(ctx, start)
	if !suite.NoError(err, "start stage") {
		suite.FailNow("failed to start stage 0")
	}
}

func (suite *LucyBatchUnarySuite) Test0150_GetJob_Started_Stage02() {
	job := suite.getJob()

	suite.checkJobBasic(job)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_RUNNING, job.Status, "status")
	suite.Equal(lucy.Result_NONE, job.Result, "result")
	suite.Equal(float32(0.5), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.checkStageCompleted(job.Stages[0])
	suite.checkStageRunning(job.Stages[1])
	// Job modified time is completed time.
	suite.True(
		job.Modified.AsTime().Equal(job.Stages[1].Started.AsTime()),
		"started equals modified",
	)
}

func (suite *LucyBatchUnarySuite) Test0160_GetBatch_Started_Stage02() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified before created",
	)

	suite.Equal(uint32(1), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(1), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.5), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0170_ProgressStage() {
	update := &lucy.ProgressStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 1,
		},
		Update: &lucy.ProgressStageUpdate{Progress: 0.5},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.ProgressStage(ctx, update)
	if !suite.NoError(err, "update progress") {
		suite.FailNow("could not update progress")
	}
}

func (suite *LucyBatchUnarySuite) Test0180_GetJob_Progressing_Stage02() {
	job := suite.getJob()

	suite.checkJobBasic(job)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_RUNNING, job.Status, "status")
	suite.Equal(lucy.Result_NONE, job.Result, "result")
	suite.Equal(float32(0.75), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.checkStageCompleted(job.Stages[0])
	suite.checkStageRunning(job.Stages[1])
	suite.Equal(float32(0.5), job.Stages[1].Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0190_GetBatch_Progressing_Stage02() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified equals created",
	)

	suite.Equal(uint32(1), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(1), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.75), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0200_CompleteStage_Success_Stage02() {
	resultData, err := anypb.New(
		&prototesting.Sorted{House: prototesting.House_Slytherin},
	)
	if !suite.NoError(err, "pack result") {
		suite.FailNow("failed to complete stage")
	}

	update := &lucy.CompleteStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 1,
		},
		Update: &lucy.CompleteStageUpdate{
			Error:      nil,
			ResultData: resultData,
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err = suite.client.CompleteStage(ctx, update)
	if !suite.NoError(err, "update progress") {
		suite.FailNow("could not update progress")
	}
}

func TestLucySuite(t *testing.T) {
	suite.Run(t, new(LucyBatchUnarySuite))
}

// ClientRunnerMock implements the lucy.LucyClient interface, but uses a
// lucy.Lucy_RunnerClient when calling the unary methods, allowing tests to be written
// for only unary methods, then ran again to test that using a RunnerStream yields
// the expected result as well.
type ClientRunnerMock struct {
	// Our embedded default client.
	lucy.LucyClient

	// The id of the last stage we sent an update for.
	lastStageId *lucy.StageID
	// Our runner.
	runner lucy.Lucy_RunnerClient
	// Lock for our runner (since gRPC streams should not be accessed concurrently)
	runnerLock *sync.Mutex
}

// updateId updates lastStageId and returns the *lucy.StageID value that should be sent
// with this request.
func (mock *ClientRunnerMock) updateId(in *lucy.StageID) *lucy.StageID {
	defer func() {
		mock.lastStageId = in
	}()

	if mock.lastStageId == nil {
		return in
	} else if in.JobId != mock.lastStageId.JobId {
		return in
	} else if in.StageIndex != mock.lastStageId.StageIndex {
		return in
	}

	return nil
}

// handleUpdateStream sends the actual update, then makes a confirmation request and
// waits for a confirmation response.
func (mock *ClientRunnerMock) handleUpdateStream(
	update *lucy.RunnerUpdate,
) error {
	// Lock the stream so we know no one else is accessing it concurrently and release
	// the lock on exit.
	mock.runnerLock.Lock()
	defer mock.runnerLock.Unlock()

	err := mock.runner.Send(update)
	if err != nil {
		return err
	}

	confirm := &lucy.RunnerUpdate{
		StageId: nil,
		Update:  &lucy.RunnerUpdate_Confirm{Confirm: new(emptypb.Empty)},
	}
	err = mock.runner.Send(confirm)
	if err != nil {
		return err
	}

	_, err = mock.runner.Recv()
	if err != nil {
		return err
	}

	return nil
}

// sendUpdate sends a runner update and returns a response as if it were a unary call.
func (mock *ClientRunnerMock) sendUpdate(
	ctx context.Context, update *lucy.RunnerUpdate,
) (_ *emptypb.Empty, err error) {
	// Run the update in it's own routine so we can watch for context cancellations.
	done := make(chan struct{})
	go func() {
		defer close(done)
		err = mock.handleUpdateStream(update)
	}()

	// Wait on either a completion or a context cancellation.
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if err != nil {
		return nil, err
	}

	return new(emptypb.Empty), nil
}

func (mock *ClientRunnerMock) StartStage(
	ctx context.Context, in *lucy.StartStage, opts ...grpc.CallOption,
) (*empty.Empty, error) {
	return mock.sendUpdate(
		ctx,
		&lucy.RunnerUpdate{
			StageId: mock.updateId(in.StageId),
			Update: &lucy.RunnerUpdate_Start{
				Start: in.Update,
			},
		},
	)
}

func (mock *ClientRunnerMock) ProgressStage(
	ctx context.Context, in *lucy.ProgressStage, opts ...grpc.CallOption,
) (*empty.Empty, error) {
	return mock.sendUpdate(
		ctx,
		&lucy.RunnerUpdate{
			StageId: mock.updateId(in.StageId),
			Update: &lucy.RunnerUpdate_Progress{
				Progress: in.Update,
			},
		},
	)
}

func (mock *ClientRunnerMock) CompleteStage(
	ctx context.Context,
	in *lucy.CompleteStage,
	opts ...grpc.CallOption,
) (*empty.Empty, error) {
	return mock.sendUpdate(
		ctx,
		&lucy.RunnerUpdate{
			StageId: mock.updateId(in.StageId),
			Update: &lucy.RunnerUpdate_Complete{
				Complete: in.Update,
			},
		},
	)
}

type LucyBatchStreamSuite struct {
	LucyBatchUnarySuite
}

func (suite *LucyBatchStreamSuite) SetupSuite() {
	suite.LucyBatchUnarySuite.SetupSuite()

	runner, err := suite.client.Runner(context.Background())
	if !suite.NoError(err, "get runner stream") {
		suite.FailNow("could not get runner stream")
	}

	suite.client = &ClientRunnerMock{
		LucyClient:  suite.client,
		lastStageId: nil,
		runner:      runner,
		runnerLock:  new(sync.Mutex),
	}
}

func (suite *LucyBatchStreamSuite) TearDownSuite() {
	mock := suite.client.(*ClientRunnerMock)
	err := mock.runner.CloseSend()
	if !suite.NoError(err, "Runner.CloseSend()") {
		suite.FailNow("could not close update stream")
	}
	suite.ManagerSuite.TearDownSuite()
}

func TestLucyBatchStreamSuite(t *testing.T) {
	suite.Run(t, new(LucyBatchStreamSuite))
}
