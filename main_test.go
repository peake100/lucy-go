package main_test

import (
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
	"os"
	"testing"
)

func init() {
	os.Setenv("LUCY_MONGO_URI", "mongodb://127.0.0.1:57017")
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

const BatchType = "SortStudents"

// This suite is going to test running batches.
type LucyBatchSuite struct {
	pktesting.ManagerSuite

	db     lucydb.LucyDB
	client lucy.LucyClient

	batchId    *cerealMessages.UUID
	batchAdded *lucy.NewBatch

	jobId    *cerealMessages.UUID
	jobAdded *lucy.NewJob
}

func (suite *LucyBatchSuite) SetupSuite() {
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

func (suite *LucyBatchSuite) Test0010NewBatch() {
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

func (suite *LucyBatchSuite) Test0020GetBatch() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	batch, err := suite.client.GetBatch(ctx, suite.batchId)
	if !suite.NoError(err, "get batch") {
		suite.FailNow("could not get batch")
	}

	suite.T().Log("RECEIVED:", batch)

	suite.Equal(suite.batchId.MustGoogle(), batch.Id.MustGoogle(), "id")
	suite.Equal(BatchType, batch.Type, "type")
	suite.Equal(suite.batchAdded.Name, batch.Name, "name")
	suite.Equal(suite.batchAdded.Description, batch.Description)

	suite.NotNil(batch.Created, "created not nil")
	suite.NotNil(batch.Modified, "modified not nil")
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
func (suite *LucyBatchSuite) Test0030AddJob() {
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

func (suite *LucyBatchSuite) Test0040GetBatch_JobPending() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	batch, err := suite.client.GetBatch(ctx, suite.batchId)
	if !suite.NoError(err, "get batch") {
		suite.FailNow("could not get batch")
	}

	suite.T().Log("RECEIVED:", batch)

	suite.Equal(suite.batchId.MustGoogle(), batch.Id.MustGoogle(), "id")
	suite.Equal(BatchType, batch.Type, "type")
	suite.Equal(suite.batchAdded.Name, batch.Name, "name")
	suite.Equal(suite.batchAdded.Description, batch.Description)

	suite.NotNil(batch.Created, "created not nil")
	suite.NotNil(batch.Modified, "modified not nil")
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"created before modified",
	)

	suite.Equal(uint32(1), batch.JobCount, "1 jobs")
	suite.Equal(uint32(1), batch.PendingCount, "1 pending")
	suite.Equal(uint32(0), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
}

func (suite *LucyBatchSuite) checkJobBasic(received *lucy.Job) {
	suite.Equal(suite.jobId.MustGoogle(), received.Id.MustGoogle())
	suite.NotNil(received.Created, "created present")
	suite.NotNil(received.Modified, "modified present")

	suite.Equal(suite.jobAdded.Type, received.Type, "type")
	suite.Equal(suite.jobAdded.Name, received.Name, "name")
	suite.Equal(
		suite.jobAdded.Description, received.Description, "description",
	)
	suite.Equal(suite.jobAdded.MaxRetries, received.MaxRetries, "max retries")


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

func (suite *LucyBatchSuite) checkStagePending(stage *lucy.JobStage) {
	suite.Nil(stage.Started, "started nil")
	suite.Nil(stage.Completed, "completed nil")
	suite.Nil(stage.ResultData, "result data nil")
	suite.Nil(stage.Error, "error nil")
	suite.Equal(lucy.Status_PENDING, stage.Status)
	suite.Equal(lucy.Result_NONE, stage.Result)
}

func (suite *LucyBatchSuite) checkStageStarted(stage *lucy.JobStage) {
	suite.NotNil(stage.Started, "started nil")
	suite.Nil(stage.Completed, "completed nil")
	suite.Nil(stage.ResultData, "result data nil")
	suite.Nil(stage.Error, "error nil")
	suite.Equal(lucy.Status_RUNNING, stage.Status)
	suite.Equal(lucy.Result_NONE, stage.Result)
}

func (suite *LucyBatchSuite) Test0050GetJob() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	job, err := suite.client.GetJob(ctx, suite.jobId)
	if !suite.NoError(err, "get job") {
		suite.FailNow("could not get job")
	}

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

func (suite *LucyBatchSuite) Test0060StartStage() {
	start := &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId: suite.jobId,
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

func (suite *LucyBatchSuite) Test0070GetJob_Started() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	job, err := suite.client.GetJob(ctx, suite.jobId)
	if !suite.NoError(err, "start stage") {
		suite.FailNow("failed to start stage 0")
	}

	suite.checkJobBasic(job)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_RUNNING, job.Status, "status")
	suite.Equal(lucy.Result_NONE, job.Result, "result")
	suite.Equal(float32(0.0), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.checkStageStarted(job.Stages[0])
	suite.checkStagePending(job.Stages[1])
}

func (suite *LucyBatchSuite) Test0080GetBatch_Started() {

}

func TestLucySuite(t *testing.T) {
	suite.Run(t, new(LucyBatchSuite))
}
