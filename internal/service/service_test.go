package service_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/gRPEAKEC-go/pktesting"
	"github.com/peake100/lucy-go/internal/db/lucymongo"
	"github.com/peake100/lucy-go/internal/messaging"
	"github.com/peake100/lucy-go/internal/prototesting"
	"github.com/peake100/lucy-go/internal/service"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"github.com/peake100/rogerRabbit-go/amqp"
	"github.com/peake100/rogerRabbit-go/amqptest"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

func init() {
	os.Setenv(lucymongo.EnvKeyMongoURI, "mongodb://127.0.0.1:57017")
	os.Setenv(messaging.EnvKeyRabbitURI, "amqp://127.0.0.1:57018")
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

const BatchType = "SortStudents"

// eventQueue holds queue information and a consumer channel for lucy events.
type eventQueue struct {
	QueueInfo       amqp.Queue
	ConsumerChannel <-chan amqp.Delivery
}

type lucySuite struct {
	// This base suite is the actual running suite.
	*suite.Suite
	// amqp holds helper methods that invoke our base suite.
	amqp amqptest.AmqpSuite
	// manager holds helper methods that invoke our base suite.
	manager pktesting.ManagerSuite

	db     lucymongo.Backend
	client lucy.LucyClient

	// eventQueueJobCreated is a queue that is declared and bound to the job creation
	// events.
	eventsJobCreated   eventQueue
	eventsBatchCreated eventQueue

	eventsStageStarted eventQueue
	eventsJobStarted   eventQueue

	eventsStageProgressed eventQueue
	eventsJobProgressed   eventQueue

	eventsStageCompleted eventQueue
	eventsJobCompleted   eventQueue

	eventsJobCancelled eventQueue

	eventsBatchUpdated eventQueue
}

func (suite *lucySuite) SetupSuite() {
	// get the dbMongo connector
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	var err error
	db, err := lucymongo.New(ctx, nil)
	suite.db = db.(lucymongo.Backend)
	if !suite.NoError(err, "connect to dbMongo") {
		suite.FailNow("could not connect to dbMongo")
	}

	// Drop the lucy database
	err = suite.db.DB.Drop(ctx)
	if !suite.NoError(err, "drop database") {
		suite.FailNow("could not drop database")
	}

	// Run the basic AMPQ setup.
	suite.setupBasicAMPQ()

	// Setup the service manager ans get it running through the manager Suite..
	manger, _ := service.NewLucyManager()
	suite.manager.Manager = manger
	suite.manager.SetupSuite()

	// Create a gRPC client connection we can use to talk to the service.
	clientConn := suite.manager.Manager.Test(suite.T()).GrpcClientConn(
		true, grpc.WithInsecure(),
	)
	suite.client = lucy.NewLucyClient(clientConn)

	// Set up the event queues so we can test them. We need to do this after the
	// manager spins up so we know the service has declared the exchanges.
	suite.setupEventQueues()
}

func (suite *lucySuite) setupBasicAMPQ() {
	logger := zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: zerolog.TimeFormatUnixMs,
	})

	config := amqp.DefaultConfig()
	config.Logger = logger
	suite.amqp.Opts = amqptest.
		NewChannelSuiteOpts().
		WithDialAddress("amqp://127.0.0.1:57018").
		WithDialConfig(config)

	suite.amqp.SetupSuite()

	amqpChan := suite.amqp.ChannelConsume()
	_ = amqpChan.ExchangeDelete(
		messaging.JobsExchange,
		false,
		false,
	)
	_ = amqpChan.ExchangeDelete(
		messaging.JobsEventsExchange,
		false,
		false,
	)
	_, _ = amqpChan.QueueDelete(
		"JOBS.SortStudent", false, false, false,
	)

	suite.amqp.ReplaceChannels()
}

func (suite *lucySuite) setupEventQueues() {
	suite.eventsBatchCreated = suite.createEventQueue(
		"TestEventsBatchCreated",
		"batch.*."+messaging.EventTypeCreated,
	)

	suite.eventsBatchUpdated = suite.createEventQueue(
		"TestEventsBatchUpdated",
		"batch.*."+messaging.EventTypeUpdated,
	)

	suite.eventsJobCreated = suite.createEventQueue(
		"TestEventsJobCreated",
		"batch.*.job.*."+messaging.EventTypeCreated,
	)

	suite.eventsStageStarted = suite.createEventQueue(
		"TestEventsStageStarted",
		"batch.*.job.*.stage.*."+messaging.EventTypeStart,
	)

	suite.eventsJobStarted = suite.createEventQueue(
		"TestEventsJobStarted",
		"batch.*.job.*."+messaging.EventTypeStart,
	)

	suite.eventsStageProgressed = suite.createEventQueue(
		"TestEventsStageProgressed",
		"batch.*.job.*.stage.*."+messaging.EventTypeProgress,
	)

	suite.eventsJobProgressed = suite.createEventQueue(
		"TestEventsJobProgressed",
		"batch.*.job.*."+messaging.EventTypeProgress,
	)

	suite.eventsStageCompleted = suite.createEventQueue(
		"TestEventsStageCompleted",
		"batch.*.job.*.stage.*."+messaging.EventTypeComplete,
	)

	suite.eventsJobCompleted = suite.createEventQueue(
		"TestEventsJobCompleted",
		"batch.*.job.*."+messaging.EventTypeComplete,
	)

	suite.eventsJobCancelled = suite.createEventQueue(
		"TestEventsJobCancelled",
		"batch.*.job.*."+messaging.EventTypeCancelled,
	)
}

func (suite *lucySuite) createEventQueue(
	queueName string, eventRoutingKey string,
) eventQueue {

	_, _ = suite.amqp.ChannelConsume().QueuePurge(
		queueName, false,
	)

	queue := suite.amqp.CreateTestQueue(
		queueName,
		messaging.JobsEventsExchange,
		eventRoutingKey,
		false,
	)

	messageChan, err := suite.amqp.ChannelConsume().Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if !suite.NoError(err, "consume job created events") {
		suite.T().FailNow()
	}

	return eventQueue{
		QueueInfo:       queue,
		ConsumerChannel: messageChan,
	}
}

func (suite *lucySuite) TearDownSuite() {
	defer suite.amqp.TearDownSuite()
	defer suite.manager.TearDownSuite()
}

func (suite *lucySuite) NewJob(studentName string) *lucy.NewJob {
	input, err := anypb.New(&prototesting.Student{Name: studentName})
	if !suite.NoError(err, "pack input") {
		suite.FailNow("could not pack job input")
	}

	job := &lucy.NewJob{
		Type:        "SortStudent",
		Name:        "Sort Student",
		Description: "sort student into a house",
		Input:       input,
		MaxRetries:  1,
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
	return job
}

func (suite *lucySuite) NewBatch(studentNames []string) *lucy.NewBatch {
	batch := &lucy.NewBatch{
		Type:        "SortStudents",
		Name:        "Sort Students",
		Description: "sort students into houses",
	}

	for _, name := range studentNames {
		batch.Jobs = append(batch.Jobs, suite.NewJob(name))
	}

	return batch
}

func newLucySuite() lucySuite {
	baseSuite := new(suite.Suite)

	newSuite := lucySuite{
		Suite:   baseSuite,
		amqp:    amqptest.NewAmqpSuite(baseSuite, nil),
		manager: pktesting.NewManagerSuite(baseSuite),
	}

	return newSuite
}

// This suite is going to test running batches.
type LucyBatchUnarySuite struct {
	lucySuite

	batchId           *cerealMessages.UUID
	batchAdded        *lucy.NewBatch
	lastBatchModified time.Time

	jobId01    *cerealMessages.UUID
	jobAdded01 *lucy.NewJob

	jobId02    *cerealMessages.UUID
	jobAdded02 *lucy.NewJob

	batch02Created *lucy.CreatedBatch
	batch03Created *lucy.CreatedBatch
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

func (suite *LucyBatchUnarySuite) checkJobBasic(
	added *lucy.NewJob,
	jobId *cerealMessages.UUID,
	received *lucy.Job,
	studentName string,
) {
	suite.Equal(jobId.MustGoogle(), received.Id.MustGoogle())
	suite.NotNil(received.Created, "created present")
	suite.NotNil(received.Modified, "modified present")

	suite.Equal(added.Type, received.Type, "type")
	suite.Equal(added.Name, received.Name, "name")
	suite.Equal(added.Description, received.Description, "description")
	suite.Equal(added.MaxRetries, received.MaxRetries, "max retries")

	msg, err := received.Input.UnmarshalNew()
	if !suite.NoError(err, "unmarshall job info") {
		suite.FailNow("could not unmarshall job info")
	}

	student, ok := msg.(*prototesting.Student)
	if !ok {
		suite.T().Error("input is not student")
		suite.FailNow("bad job input data")
	}

	suite.Equal(studentName, student.Name, "input data")

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

func (suite *LucyBatchUnarySuite) getJob(jobId *cerealMessages.UUID) *lucy.Job {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	job, err := suite.client.GetJob(ctx, jobId)
	if !suite.NoError(err, "get job") {
		suite.FailNow("could not get job")
	}
	return job
}

func (suite *LucyBatchUnarySuite) Test0010_NewBatch() {
	batch := &lucy.NewBatch{
		Type:        BatchType,
		Name:        "Sort Students",
		Description: "send new students through the sorting hat",
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	created, err := suite.client.CreateBatch(ctx, batch)
	if !suite.NoError(err, "new batch id") {
		suite.FailNow("could not create batch")
	}

	suite.batchId = created.BatchId
	suite.batchAdded = batch

	suite.T().Log("NEW BATCH ID:", suite.batchId.MustGoogle())
}

// getEvent gets an event message from an amqp message consumer channel, and failing the
// test if any errors occur.
//
// The message will be decoded into the event value, which will then be returned.
func (suite *LucyBatchUnarySuite) getEvent(
	events eventQueue, event proto.Message, isLast bool,
) (msg amqp.Delivery, decoded proto.Message) {
	// Get the event or report a timeout after 3 seconds.
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()

	var ok bool
	select {
	case msg, ok = <-events.ConsumerChannel:
		suite.True(ok, "event channel open")
	case <-timeout.C:
		suite.T().Errorf("timeout waiting for event")
		suite.T().FailNow()
	}

	// Unmarshal the message into the event passed by the caller.
	err := proto.Unmarshal(msg.Body, event)
	if !suite.NoError(err, "decode event") {
		suite.T().FailNow()
	}

	// If this event has a modified value, check that it is within a second of the
	// truncated amqp modified time.
	type hasModified interface {
		GetModified() *timestamppb.Timestamp
	}

	eventModified, ok := event.(hasModified)
	if !ok {
		return msg, event
	}

	// AMPQ timestamps are rounded to the nearest second, so these values should be
	// within a second of each other.
	suite.WithinDuration(
		eventModified.GetModified().AsTime(), msg.Timestamp.UTC(), 1*time.Second,
	)

	if !isLast {
		return
	}

	// If we are expecting this to be the last event queued, confirm there are no
	// more waiting in the queue.
	queueInfo, err := suite.amqp.ChannelConsume().QueueInspect(events.QueueInfo.Name)
	if !suite.NoError(err, "inspect queue") {
		suite.T().FailNow()
	}

	// Make sure there are no undelivered messages
	suite.Equal(
		0,
		queueInfo.Messages,
		"0 remaining messages in event queue",
	)

	select {
	case <-events.ConsumerChannel:
		suite.T().Errorf(
			"got extra event from queue. Event queue should be emtpy",
		)
	default:
	}

	return msg, event
}

func (suite *LucyBatchUnarySuite) checkEventStageId(
	expected *events.StageId, received *events.StageId,
) {
	suite.Equal(
		expected.BatchId.MustGoogle().String(), received.BatchId.MustGoogle().String(),
		"batch id",
	)
	suite.Equal(
		expected.JobId.MustGoogle().String(), received.JobId.MustGoogle().String(),
		"job id",
	)
	suite.Equal(
		expected.StageIndex, received.StageIndex,
		"stage index",
	)
}

func (suite *LucyBatchUnarySuite) checkEventJobId(
	expected *events.JobId, received *events.JobId,
) {
	suite.Equal(
		expected.BatchId.MustGoogle().String(), received.BatchId.MustGoogle().String(),
		"batch id",
	)
	suite.Equal(
		expected.JobId.MustGoogle().String(), received.JobId.MustGoogle().String(),
		"job id",
	)
}

func (suite *LucyBatchUnarySuite) Test0015_NewBatch_GetEvent() {
	event := new(events.BatchCreated)
	_, _ = suite.getEvent(suite.eventsBatchCreated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)
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
	job := suite.NewJob("Harry Potter")

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

	suite.jobId01 = created.Ids[0]
	suite.jobAdded01 = job
}

func (suite *LucyBatchUnarySuite) Test0033_GetMessageFromWorkerQueue_Job01() {
	queue, err := suite.amqp.ChannelConsume().QueueDeclare(
		"JOBS.SortStudent",
		true,
		false,
		false,
		false,
		nil,
	)

	if !suite.NoError(err, "queue declare") {
		suite.T().FailNow()
	}

	err = suite.amqp.ChannelConsume().QueueBind(
		queue.Name,
		"SortStudent",
		messaging.JobsExchange,
		false,
		nil,
	)
	if !suite.NoError(err, "bind queue") {
		suite.T().FailNow()
	}

	delivery := suite.amqp.GetMessage(queue.Name, true)
	if !suite.NoError(err, "get message") {
		suite.T().FailNow()
	}

	jobId := new(cerealMessages.UUID)
	err = proto.Unmarshal(delivery.Body, jobId)
	if !suite.NoError(err, "unmarshal delivery body") {
		suite.T().FailNow()
	}

	suite.Equal(
		suite.jobId01.MustGoogle().String(),
		jobId.MustGoogle().String(),
		"job id expected",
	)
}

func (suite *LucyBatchUnarySuite) Test0037_GetEventCreated_Job01() {
	event := new(events.JobCreated)
	suite.getEvent(suite.eventsJobCreated, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId01,
		},
		event.Id,
	)
}

func (suite *LucyBatchUnarySuite) Test0038_GetBatchEvent_Job01Created() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(1), event.JobCount, "1 jobs")
	suite.Equal(uint32(1), event.PendingCount, "1 pending")
	suite.Equal(uint32(0), event.RunningCount, "no running")
	suite.Equal(uint32(0), event.CompletedCount, "no completed")
	suite.Equal(uint32(0), event.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "no successes")
	suite.Equal(uint32(0), event.FailureCount, "no failures")
	suite.Equal(float32(0.0), event.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0040_GetBatch_JobPending() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified equals created",
	)

	suite.Equal(uint32(1), batch.JobCount, "1 jobs")
	suite.Equal(uint32(1), batch.PendingCount, "1 pending")
	suite.Equal(uint32(0), batch.RunningCount, "no running")
	suite.Equal(uint32(0), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.0), batch.Progress, "progress: 0.0")

	if !suite.Len(batch.Jobs, 1, "job ids length") {
		suite.T().FailNow()
	}

	suite.Equal(
		suite.jobId01.MustGoogle(),
		batch.Jobs[0].MustGoogle(),
		"job id correct",
	)
}

func (suite *LucyBatchUnarySuite) Test0050_GetJob_Pending() {
	job := suite.getJob(suite.jobId01)

	suite.checkJobBasic(
		suite.jobAdded01, suite.jobId01, job, "Harry Potter",
	)
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
			JobId:      suite.jobId01,
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

func (suite *LucyBatchUnarySuite) Test0065_GetStageStartEvent() {
	event := new(events.StageStart)
	_, _ = suite.getEvent(suite.eventsStageStarted, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId01,
			StageIndex: 0,
		},
		event.Id,
	)

	suite.Equal("testworker", event.RunBy, "run_by")
	suite.Equal(uint32(1), event.RunCount, "run_count")
}

func (suite *LucyBatchUnarySuite) Test0066_GetJobStartEvent() {
	event := new(events.JobStart)
	_, _ = suite.getEvent(suite.eventsJobStarted, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId01,
		},
		event.Id,
	)

	suite.Equal("testworker", event.RunBy, "run_by")
	suite.Equal(uint32(1), event.RunCount, "run_count")
}

func (suite *LucyBatchUnarySuite) Test0067_GetBatchEvent_Stage01Started() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(1), event.JobCount, "1 jobs")
	suite.Equal(uint32(0), event.PendingCount, "no pending")
	suite.Equal(uint32(1), event.RunningCount, "1 running")
	suite.Equal(uint32(0), event.CompletedCount, "no completed")
	suite.Equal(uint32(0), event.CancelledCount, "no cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "no successes")
	suite.Equal(uint32(0), event.FailureCount, "no failures")
	suite.Equal(float32(0.0), event.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0070_GetJob_Started() {
	job := suite.getJob(suite.jobId01)

	suite.checkJobBasic(
		suite.jobAdded01, suite.jobId01, job, "Harry Potter",
	)
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
			JobId:      suite.jobId01,
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

func (suite *LucyBatchUnarySuite) Test0095_GetProgressStageEvent() {
	event := new(events.StageProgress)
	_, _ = suite.getEvent(suite.eventsStageProgressed, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId01,
			StageIndex: 0,
		},
		event.Id,
	)

	suite.Equal(float32(0.5), event.Progress)
}

func (suite *LucyBatchUnarySuite) Test0096_GetBatchEvent_Stage01Progressed() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(1), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(1), event.RunningCount, "running")
	suite.Equal(uint32(0), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0.25), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0096_GetProgressJobEvent() {
	event := new(events.JobProgress)
	_, _ = suite.getEvent(suite.eventsJobProgressed, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId01,
		},
		event.Id,
	)

	suite.Equal(float32(0.25), event.Progress)
}

func (suite *LucyBatchUnarySuite) Test0100_GetJob_Progressing() {
	job := suite.getJob(suite.jobId01)

	suite.checkJobBasic(
		suite.jobAdded01, suite.jobId01, job, "Harry Potter",
	)
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
			JobId:      suite.jobId01,
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

func (suite *LucyBatchUnarySuite) Test0125_GetCompleteStageEvent() {
	event := new(events.StageComplete)
	_, _ = suite.getEvent(suite.eventsStageCompleted, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId01,
			StageIndex: 0,
		},
		event.Id,
	)

	suite.Equal(lucy.Result_SUCCEEDED, event.Result)
}

func (suite *LucyBatchUnarySuite) Test0126_GetBatchEvent_Stage01Completed() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(1), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(1), event.RunningCount, "running")
	suite.Equal(uint32(0), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0.50), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) checkStageCompleted(stage *lucy.JobStage) {
	suite.NotNil(stage.Started, "started not nil")
	suite.NotNil(stage.Completed, "completed not nil")
	suite.NotNil(stage.ResultData, "result data nil")
	suite.Nil(stage.Error, "error nil")
	suite.Equal(lucy.Status_COMPLETED, stage.Status, "status")
	suite.Equal(lucy.Result_SUCCEEDED, stage.Result, "result")
	suite.Equal(stage.Progress, float32(1.0), "progress")
}

func (suite *LucyBatchUnarySuite) Test0130_GetJob_OneCompleted() {
	job := suite.getJob(suite.jobId01)

	suite.checkJobBasic(
		suite.jobAdded01, suite.jobId01, job, "Harry Potter",
	)
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
			JobId:      suite.jobId01,
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

func (suite *LucyBatchUnarySuite) Test0145_GetStartStage02Event() {
	event := new(events.StageStart)
	_, _ = suite.getEvent(suite.eventsStageStarted, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId01,
			StageIndex: 1,
		},
		event.Id,
	)

	suite.Equal("testworker", event.RunBy)
}

func (suite *LucyBatchUnarySuite) Test0150_GetJob_Started_Stage02() {
	job := suite.getJob(suite.jobId01)

	suite.checkJobBasic(
		suite.jobAdded01, suite.jobId01, job, "Harry Potter",
	)
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
			JobId:      suite.jobId01,
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

func (suite *LucyBatchUnarySuite) Test0175_GetProgressEvent_Stage02() {
	event := new(events.StageProgress)
	_, _ = suite.getEvent(suite.eventsStageProgressed, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId01,
			StageIndex: 1,
		},
		event.Id,
	)

	suite.Equal(float32(0.5), event.Progress)
}

func (suite *LucyBatchUnarySuite) Test0176_GetBatchEvent_Stage02Progressed() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(1), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(1), event.RunningCount, "running")
	suite.Equal(uint32(0), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0.75), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0176_GetProgressJobEvent_Stage02() {
	event := new(events.JobProgress)
	_, _ = suite.getEvent(suite.eventsJobProgressed, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId01,
		},
		event.Id,
	)

	suite.Equal(float32(0.75), event.Progress)
}

func (suite *LucyBatchUnarySuite) Test0180_GetJob_Progressing_Stage02() {
	job := suite.getJob(suite.jobId01)

	suite.checkJobBasic(
		suite.jobAdded01, suite.jobId01, job, "Harry Potter",
	)
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
			JobId:      suite.jobId01,
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

func (suite *LucyBatchUnarySuite) Test0205_GetCompleteStage02Event() {
	event := new(events.StageComplete)
	_, _ = suite.getEvent(suite.eventsStageCompleted, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId01,
			StageIndex: 1,
		},
		event.Id,
	)

	suite.Equal(lucy.Result_SUCCEEDED, event.Result)
}

func (suite *LucyBatchUnarySuite) Test0206_GetCompleteJobEvent_Job01() {
	event := new(events.JobComplete)
	_, _ = suite.getEvent(suite.eventsJobCompleted, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId01,
		},
		event.Id,
	)

	suite.Equal(lucy.Result_SUCCEEDED, event.Result)
}

func (suite *LucyBatchUnarySuite) Test0207_GetBatchEvent_Job01Success() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(1), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(1), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(1), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(1.0), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0210_GetJob_Completed() {
	job := suite.getJob(suite.jobId01)

	suite.checkJobBasic(
		suite.jobAdded01, suite.jobId01, job, "Harry Potter",
	)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified equals created",
	)
	suite.Equal(lucy.Status_COMPLETED, job.Status, "status")
	suite.Equal(lucy.Result_SUCCEEDED, job.Result, "result")
	suite.Equal(float32(1.0), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.checkStageCompleted(job.Stages[0])
	suite.checkStageCompleted(job.Stages[1])

	// Job modified time is completed time.
	suite.True(
		job.Modified.AsTime().Equal(job.Stages[1].Completed.AsTime()),
		"completed equals modified",
	)

	resultData, err := job.Stages[1].ResultData.UnmarshalNew()
	if !suite.NoError(err, "unmarshall result data") {
		suite.FailNow("could not get stage 0 result data")
	}

	sorted, ok := resultData.(*prototesting.Sorted)
	if !ok {
		suite.FailNow("stage 0 result data is not Sorted value")
	}

	suite.Equal(prototesting.House_Slytherin, sorted.House, "result data")
}

func (suite *LucyBatchUnarySuite) Test0220_AddJobToBatch() {
	job := suite.NewJob("Hermione Granger")
	req := &lucy.NewJobs{
		Batch: suite.batchId,
		Jobs:  []*lucy.NewJob{job},
	}

	suite.jobAdded02 = job

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()
	created, err := suite.client.CreateJobs(ctx, req)
	if !suite.NoError(err, "add new job") {
		suite.T().FailNow()
	}

	if !suite.Len(created.Ids, 1, "1 id returned") {
		suite.T().FailNow()
	}

	suite.NotNil(created.Ids[0], "id is not nil")
	suite.jobId02 = created.Ids[0]
}

func (suite *LucyBatchUnarySuite) Test0225_GetMessageFromWorkerQueue_Job02() {
	message := suite.amqp.GetMessage(messaging.WorkerQueueName(
		"SortStudent"), true,
	)

	jobId := new(cerealMessages.UUID)
	err := proto.Unmarshal(message.Body, jobId)
	if !suite.NoError(err, "unmarshal job id") {
		suite.T().FailNow()
	}

	suite.Equal(suite.jobId02.MustGoogle(), jobId.MustGoogle(), "job id")
}

func (suite *LucyBatchUnarySuite) Test0226_Batch01_GetEvent_Job02Added() {
	event := new(events.JobCreated)
	_, _ = suite.getEvent(suite.eventsJobCreated, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId02,
		},
		event.Id,
	)
}

func (suite *LucyBatchUnarySuite) Test0227_GetBatchEvent_Job02Added() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(1), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(1), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(1), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0.5), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0230_GetBatch_Job02Pending() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified equals created",
	)

	suite.Equal(uint32(2), batch.JobCount, "no jobs")
	suite.Equal(uint32(1), batch.PendingCount, "no pending")
	suite.Equal(uint32(0), batch.RunningCount, "no running")
	suite.Equal(uint32(1), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(1), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.5), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0240_GetJob02_Pending() {
	job := suite.getJob(suite.jobId02)

	suite.checkJobBasic(
		suite.jobAdded02, suite.jobId02, job, "Hermione Granger",
	)
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

func (suite *LucyBatchUnarySuite) Test0250_StartStage01_Job02() {
	start := &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId02,
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

func (suite *LucyBatchUnarySuite) Test0255_GetStartStage01Event_Job02() {
	event := new(events.StageStart)
	_, _ = suite.getEvent(suite.eventsStageStarted, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId02,
			StageIndex: 0,
		},
		event.Id,
	)

	suite.Equal("testworker", event.RunBy)
}

func (suite *LucyBatchUnarySuite) Test0256_GetJobStartEvent_Job02() {
	event := new(events.JobStart)
	_, _ = suite.getEvent(suite.eventsJobStarted, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId02,
		},
		event.Id,
	)

	suite.Equal("testworker", event.RunBy, "run_by")
	suite.Equal(uint32(1), event.RunCount, "run_count")
}

func (suite *LucyBatchUnarySuite) Test0257_GetBatchEvent_Job02Started() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(1), event.RunningCount, "running")
	suite.Equal(uint32(1), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(1), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0.5), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0260_GetJob02_Started() {
	job := suite.getJob(suite.jobId02)

	suite.checkJobBasic(
		suite.jobAdded02, suite.jobId02, job, "Hermione Granger",
	)
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

func (suite *LucyBatchUnarySuite) Test0270_GetBatch_Started_Job02() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified before created",
	)

	suite.Equal(uint32(2), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(1), batch.RunningCount, "no running")
	suite.Equal(uint32(1), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(1), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.5), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0280_ProgressStage_Job02() {
	update := &lucy.ProgressStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId02,
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

func (suite *LucyBatchUnarySuite) Test0285_GetProgressStageEvent_Job02() {
	event := new(events.StageProgress)
	_, _ = suite.getEvent(suite.eventsStageProgressed, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId02,
			StageIndex: 0,
		},
		event.Id,
	)

	suite.Equal(float32(0.5), event.Progress)
}

func (suite *LucyBatchUnarySuite) Test0286_GetProgressJobEvent_Job02() {
	event := new(events.JobProgress)
	_, _ = suite.getEvent(suite.eventsJobProgressed, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId02,
		},
		event.Id,
	)

	suite.Equal(float32(0.25), event.Progress)
}

func (suite *LucyBatchUnarySuite) Test0287_GetBatchEvent_Job02Progressed() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(1), event.RunningCount, "running")
	suite.Equal(uint32(1), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(1), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0.625), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0290_GetJob02_Progressing() {
	job := suite.getJob(suite.jobId02)

	suite.checkJobBasic(
		suite.jobAdded02, suite.jobId02, job, "Hermione Granger",
	)
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

func (suite *LucyBatchUnarySuite) Test0300_GetBatch_Progressing_Job02() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified equals created",
	)

	suite.Equal(uint32(2), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(1), batch.RunningCount, "no running")
	suite.Equal(uint32(1), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(1), batch.SuccessCount, "no successes")
	suite.Equal(uint32(0), batch.FailureCount, "no failures")
	suite.Equal(float32(0.625), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0310_CompleteStage_Failed_Job02() {
	update := &lucy.CompleteStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId02,
			StageIndex: 0,
		},
		Update: &lucy.CompleteStageUpdate{
			Error: &pkerr.Error{
				Id:          cerealMessages.MustUUIDRandom(),
				Issuer:      "Hogwarts",
				Code:        934,
				GrpcCode:    int32(codes.InvalidArgument),
				Name:        "ValidationError",
				Message:     "house elves cannot be students",
				SourceError: "",
				SourceType:  "",
				Time:        timestamppb.New(time.Now().UTC()),
				Details:     nil,
				Trace:       nil,
			},
			ResultData: nil,
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.CompleteStage(ctx, update)
	if !suite.NoError(err, "update progress") {
		suite.FailNow("could not update progress")
	}
}

func (suite *LucyBatchUnarySuite) Test0315_GetCompleteStageEvent_Job02() {
	event := new(events.StageComplete)
	_, _ = suite.getEvent(suite.eventsStageCompleted, event, true)

	suite.checkEventStageId(
		&events.StageId{
			BatchId:    suite.batchId,
			JobId:      suite.jobId02,
			StageIndex: 0,
		},
		event.Id,
	)

	suite.Equal(lucy.Result_FAILED, event.Result)
}

func (suite *LucyBatchUnarySuite) Test0316_GetCompleteJobEvent_Job02_Failed() {
	event := new(events.JobComplete)
	_, _ = suite.getEvent(suite.eventsJobCompleted, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batchId,
			JobId:   suite.jobId02,
		},
		event.Id,
	)

	suite.Equal(lucy.Result_FAILED, event.Result)
}

func (suite *LucyBatchUnarySuite) Test0317_GetBatchEvent_Job02Failed() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(2), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(1), event.SuccessCount, "successes")
	suite.Equal(uint32(1), event.FailureCount, "failures")
	suite.Equal(float32(0.625), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0320_GetJob_Failed_Job02() {
	job := suite.getJob(suite.jobId02)

	suite.checkJobBasic(
		suite.jobAdded02, suite.jobId02, job, "Hermione Granger",
	)
	suite.True(
		job.Modified.AsTime().After(job.Created.AsTime()),
		"modified after created",
	)
	suite.Equal(lucy.Status_COMPLETED, job.Status, "status")
	suite.Equal(lucy.Result_FAILED, job.Result, "result")
	suite.Equal(float32(0.25), job.Progress, "progress")
	suite.Equal(uint32(1), job.RunCount, "run count")

	suite.Equal(lucy.Status_COMPLETED, job.Stages[0].Status, "stage status")
	suite.Equal(lucy.Result_FAILED, job.Stages[0].Result, "stage result")
	suite.Equal(float32(0.5), job.Stages[0].Progress, "stage result")

	suite.checkStagePending(job.Stages[1])

	// Job modified time is completed time.
	suite.True(
		job.Modified.AsTime().Equal(job.Stages[0].Completed.AsTime()),
		"completed equals modified",
	)

	suite.NotNil(job.Stages[0].Error, "error not nil")
	suite.Nil(job.Stages[0].ResultData, "result data is nil")
}

func (suite *LucyBatchUnarySuite) Test0330_GetBatch_Failed_Job02() {
	batch := suite.getBatch()

	suite.T().Log("RECEIVED:", batch)

	suite.checkBatchBasic(batch)
	suite.True(
		batch.Created.AsTime().Before(batch.Modified.AsTime()),
		"modified after created",
	)

	suite.Equal(uint32(2), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(0), batch.RunningCount, "no running")
	suite.Equal(uint32(2), batch.CompletedCount, "no completed")
	suite.Equal(uint32(0), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(1), batch.SuccessCount, "no successes")
	suite.Equal(uint32(1), batch.FailureCount, "no failures")
	suite.Equal(float32(0.625), batch.Progress, "progress: 0.0")
}

func (suite *LucyBatchUnarySuite) Test0331_Cancel_CompletedBatch() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()
	_, err := suite.client.CancelBatches(
		ctx, &lucy.CancelBatches{BatchIds: []*cerealMessages.UUID{suite.batchId}},
	)

	if !suite.NoError(err, "cancel batch") {
		suite.T().FailNow()
	}

	batch := suite.getBatch()
	suite.checkBatchBasic(batch)

	suite.Equal(uint32(2), batch.JobCount, "no jobs")
	suite.Equal(uint32(0), batch.PendingCount, "no pending")
	suite.Equal(uint32(0), batch.RunningCount, "no running")
	suite.Equal(uint32(1), batch.CompletedCount, "no completed")
	suite.Equal(uint32(1), batch.CancelledCount, "no cancelled")
	suite.Equal(uint32(1), batch.SuccessCount, "no successes")
	suite.Equal(uint32(1), batch.FailureCount, "no failures")
	suite.Equal(float32(0.625), batch.Progress, "progress: 0.0")

	jobs, err := suite.client.GetBatchJobs(ctx, batch.Id)
	if !suite.NoError(err, "get batch jobs") {
		suite.T().FailNow()
	}

	suite.Equal(
		lucy.Status_COMPLETED, jobs.Jobs[0].Status, "job 1 completed",
	)
	suite.Equal(
		lucy.Result_SUCCEEDED, jobs.Jobs[0].Result, "job 1 success",
	)
	suite.checkStageCompleted(jobs.Jobs[0].Stages[0])
	suite.checkStageCompleted(jobs.Jobs[0].Stages[1])

	suite.Equal(
		lucy.Status_CANCELLED, jobs.Jobs[1].Status, "job 0 cancelled",
	)
	suite.Equal(
		lucy.Result_FAILED, jobs.Jobs[1].Result, "job 0 failed",
	)
	suite.NotNil(jobs.Jobs[1].Stages[0].Error, "error not nil")
}

func (suite *LucyBatchUnarySuite) Test0332_GetBatchEvent_JobCancelled() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(1), event.CompletedCount, "completed")
	suite.Equal(uint32(1), event.CancelledCount, "cancelled")
	suite.Equal(uint32(1), event.SuccessCount, "successes")
	suite.Equal(uint32(1), event.FailureCount, "failures")
	suite.Equal(float32(0.625), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0333_GetJobEvent_JobCancelled() {
	event := new(events.JobCancelled)
	suite.getEvent(suite.eventsJobCancelled, event, true)

	suite.Equal(
		suite.batchId.MustGoogle().String(),
		event.Id.BatchId.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(
		suite.jobId02.MustGoogle().String(),
		event.Id.JobId.MustGoogle().String(),
		"job id",
	)
}

func (suite *LucyBatchUnarySuite) Test0340_CreateBatch02_MultiJob() {
	batch := &lucy.NewBatch{
		Type:        "SortStudents",
		Name:        "Sort Students",
		Description: "sort students into batched",
		Jobs: []*lucy.NewJob{
			suite.NewJob("Neville Longbottom"),
			suite.NewJob("Ron Weasley"),
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()
	created, err := suite.client.CreateBatch(ctx, batch)
	if !suite.NoError(err, "error creating batch") {
		suite.T().FailNow()
	}
	suite.batch02Created = created
	fmt.Println("ID:", suite.batch02Created.BatchId.MustGoogle())
}

func (suite *LucyBatchUnarySuite) Test0341_Batch02_GetEvent_Created() {
	event := new(events.BatchCreated)
	_, _ = suite.getEvent(suite.eventsBatchCreated, event, true)

	suite.Equal(
		suite.batch02Created.BatchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)
}

func (suite *LucyBatchUnarySuite) Test0342_Batch02_GetEvents_JobCreated01() {
	event := new(events.JobCreated)
	_, _ = suite.getEvent(suite.eventsJobCreated, event, false)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batch02Created.BatchId,
			JobId:   suite.batch02Created.JobIds[0],
		},
		event.Id,
	)
}

func (suite *LucyBatchUnarySuite) Test0343_Batch02_GetEvents_JobCreated02() {
	event := new(events.JobCreated)
	_, _ = suite.getEvent(suite.eventsJobCreated, event, true)

	suite.checkEventJobId(
		&events.JobId{
			BatchId: suite.batch02Created.BatchId,
			JobId:   suite.batch02Created.JobIds[1],
		},
		event.Id,
	)
}

func (suite *LucyBatchUnarySuite) Test0344_GetBatchEvent_Batch02JobsAdded() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batch02Created.BatchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(2), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(0), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0345_GetBatch02_MultiJob() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	batch, err := suite.client.GetBatch(ctx, suite.batch02Created.BatchId)
	if !suite.NoError(err, "get batch") {
		suite.T().FailNow()
	}

	if !suite.Len(batch.Jobs, 2, "2 job ids present") {
		suite.T().FailNow()
	}
}

func (suite *LucyBatchUnarySuite) Test0347_GetBatch02_WorkQueueMessages() {
	msg := suite.amqp.GetMessage(
		messaging.WorkerQueueName("SortStudent"), true,
	)

	jobIds := make([]string, 0)

	jobId := new(cerealMessages.UUID)
	err := proto.Unmarshal(msg.Body, jobId)
	if !suite.NoError(err, "unmarshal job 1 id") {
		suite.T().FailNow()
	}
	jobIds = append(jobIds, jobId.MustGoogle().String())

	msg = suite.amqp.GetMessage(
		messaging.WorkerQueueName("SortStudent"), true,
	)

	jobId2 := new(cerealMessages.UUID)
	err = proto.Unmarshal(msg.Body, jobId2)
	if !suite.NoError(err, "unmarshal job 1 id") {
		suite.T().FailNow()
	}
	jobIds = append(jobIds, jobId2.MustGoogle().String())

	suite.Contains(
		jobIds,
		suite.batch02Created.JobIds[0].MustGoogle().String(),
		"job 1 id",
	)

	suite.Contains(
		jobIds,
		suite.batch02Created.JobIds[1].MustGoogle().String(),
		"job 2 id",
	)
}

func (suite *LucyBatchUnarySuite) Test0350_CancelBatch() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.CancelBatches(
		ctx,
		&lucy.CancelBatches{
			BatchIds: []*cerealMessages.UUID{suite.batch02Created.BatchId},
		},
	)
	if !suite.NoError(err, "cancel batch") {
		suite.T().FailNow()
	}
}

func (suite *LucyBatchUnarySuite) Test0351_GetBatchEvent_Batch02Cancelled() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batch02Created.BatchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(0), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(0), event.CompletedCount, "completed")
	suite.Equal(uint32(2), event.CancelledCount, "cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0352_GetJobEvent_Batch02Job01_Cancelled() {
	event := new(events.JobCancelled)
	suite.getEvent(suite.eventsJobCancelled, event, false)

	suite.Equal(
		suite.batch02Created.BatchId.MustGoogle().String(),
		event.Id.BatchId.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(
		suite.batch02Created.JobIds[0].MustGoogle().String(),
		event.Id.JobId.MustGoogle().String(),
		"job id",
	)
}

func (suite *LucyBatchUnarySuite) Test0352_GetJobEvent_Batch02Job02_Cancelled() {
	event := new(events.JobCancelled)
	suite.getEvent(suite.eventsJobCancelled, event, true)

	suite.Equal(
		suite.batch02Created.BatchId.MustGoogle().String(),
		event.Id.BatchId.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(
		suite.batch02Created.JobIds[1].MustGoogle().String(),
		event.Id.JobId.MustGoogle().String(),
		"job id",
	)
}

func (suite *LucyBatchUnarySuite) Test0360_GetBatch02_Cancelled() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	batch, err := suite.client.GetBatch(ctx, suite.batch02Created.BatchId)
	if !suite.NoError(err, "get cancelled batch") {
		suite.T().FailNow()
	}

	suite.Equal(uint32(2), batch.CancelledCount, "cancelled_count")
	suite.Equal(uint32(2), batch.JobCount, "job_count")
	suite.Equal(uint32(0), batch.RunningCount, "running_count")
	suite.Equal(uint32(0), batch.PendingCount, "pending_count")
	suite.Equal(uint32(0), batch.CompletedCount, "completed_count")
	suite.Equal(uint32(0), batch.SuccessCount, "success_count")
	suite.Equal(uint32(0), batch.FailureCount, "failure_count")
	suite.Len(batch.Jobs, 2, "two batch jobs")
}

func (suite *LucyBatchUnarySuite) Test0370_GetJobs_Cancelled() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	jobs, err := suite.client.GetBatchJobs(ctx, suite.batch02Created.BatchId)
	if !suite.NoError(err, "get cancelled batch jobs") {
		suite.T().FailNow()
	}

	if !suite.Len(jobs.Jobs, 2, "two batch jobs") {
		suite.T().FailNow()
	}

	for i, thisJob := range jobs.Jobs {
		suite.Equalf(
			lucy.Status_CANCELLED, thisJob.Status, "job %v is cancelled", i,
		)

		for j, thisStage := range thisJob.Stages {
			suite.Equalf(
				lucy.Status_CANCELLED, thisStage.Status,
				"job %v, stage %v is cancelled",
				i, j,
			)
		}
	}
}

func (suite *LucyBatchUnarySuite) Test0380_CancelJob() {
	batch := &lucy.NewBatch{
		Type:        "SortStudents",
		Name:        "Sort Students",
		Description: "sort students into batched",
		Jobs: []*lucy.NewJob{
			suite.NewJob("Neville Longbottom"),
			suite.NewJob("Ron Weasley"),
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()
	created, err := suite.client.CreateBatch(ctx, batch)
	if !suite.NoError(err, "error creating batch") {
		suite.T().FailNow()
	}

	suite.batch03Created = created

	_, err = suite.client.CancelJob(ctx, created.JobIds[0])
	if !suite.NoError(err, "cancel first job") {
		suite.T().FailNow()
	}
}

func (suite *LucyBatchUnarySuite) Test0381_GetBatchEvent_Batch03Created() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, false)

	suite.Equal(
		suite.batch03Created.BatchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(2), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(0), event.CompletedCount, "completed")
	suite.Equal(uint32(0), event.CancelledCount, "cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0382_GetBatchEvent_Batch03Cancelled() {
	event := new(events.BatchUpdated)
	suite.getEvent(suite.eventsBatchUpdated, event, true)

	suite.Equal(
		suite.batch03Created.BatchId.MustGoogle().String(),
		event.Id.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(uint32(2), event.JobCount, "jobs")
	suite.Equal(uint32(1), event.PendingCount, "pending")
	suite.Equal(uint32(0), event.RunningCount, "running")
	suite.Equal(uint32(0), event.CompletedCount, "completed")
	suite.Equal(uint32(1), event.CancelledCount, "cancelled")
	suite.Equal(uint32(0), event.SuccessCount, "successes")
	suite.Equal(uint32(0), event.FailureCount, "failures")
	suite.Equal(float32(0), event.Progress, "progress")
}

func (suite *LucyBatchUnarySuite) Test0383_GetJobEvent_Batch03Job01_Created() {
	event := new(events.JobCancelled)
	suite.getEvent(suite.eventsJobCreated, event, false)

	suite.Equal(
		suite.batch03Created.BatchId.MustGoogle().String(),
		event.Id.BatchId.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(
		suite.batch03Created.JobIds[0].MustGoogle().String(),
		event.Id.JobId.MustGoogle().String(),
		"job id",
	)
}

func (suite *LucyBatchUnarySuite) Test0384_GetJobEvent_Batch03Job02_Created() {
	event := new(events.JobCancelled)
	suite.getEvent(suite.eventsJobCreated, event, true)

	suite.Equal(
		suite.batch03Created.BatchId.MustGoogle().String(),
		event.Id.BatchId.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(
		suite.batch03Created.JobIds[1].MustGoogle().String(),
		event.Id.JobId.MustGoogle().String(),
		"job id",
	)
}

func (suite *LucyBatchUnarySuite) Test0385_GetJobEvent_Batch03Job01_Cancelled() {
	event := new(events.JobCancelled)
	suite.getEvent(suite.eventsJobCancelled, event, true)

	suite.Equal(
		suite.batch03Created.BatchId.MustGoogle().String(),
		event.Id.BatchId.MustGoogle().String(),
		"batch id",
	)

	suite.Equal(
		suite.batch03Created.JobIds[0].MustGoogle().String(),
		event.Id.JobId.MustGoogle().String(),
		"job id",
	)
}

func (suite *LucyBatchUnarySuite) Test0386_GetBatch03_CancelledJob() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	jobs, err := suite.client.GetBatchJobs(ctx, suite.batch03Created.BatchId)
	if !suite.NoError(err, "get batch jobs") {
		suite.T().FailNow()
	}

	suite.Equal(
		lucy.Status_CANCELLED, jobs.Jobs[0].Status, "job 0 cancelled",
	)
	suite.Equal(lucy.Status_PENDING, jobs.Jobs[1].Status, "job 1 pending")

	for i, thisStage := range jobs.Jobs[0].Stages {
		suite.Equalf(
			lucy.Status_CANCELLED, thisStage.Status, "stage %v cancelled", i,
		)
	}

	for i, thisStage := range jobs.Jobs[1].Stages {
		suite.Equalf(
			lucy.Status_PENDING, thisStage.Status, "stage %v pending", i,
		)
	}
}

func (suite *LucyBatchUnarySuite) Test0390_ListBatches() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	stream, err := suite.client.ListBatches(ctx, new(emptypb.Empty))
	if !suite.NoError(err, "get stream") {
		suite.T().FailNow()
	}

	expectedBatchIds := []*cerealMessages.UUID{
		suite.batch03Created.BatchId,
		suite.batch02Created.BatchId,
		suite.batchId,
	}

	var i int
	for i = 0; ; i++ {
		thisBatch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if !suite.NoError(err, "get batch") {
			suite.T().FailNow()
		}

		if !suite.Less(
			i, len(expectedBatchIds), "more batches than expected",
		) {
			suite.T().FailNow()
		}

		suite.Equalf(
			expectedBatchIds[i].MustGoogle().String(),
			thisBatch.Id.MustGoogle().String(),
			"batch id %v expected", i,
		)
	}

	suite.Equalf(3, i, "3 batches found")
}

func TestLucyBatchUnarySuite(t *testing.T) {
	suite.Run(t, &LucyBatchUnarySuite{
		lucySuite: newLucySuite(),
	})
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
	// Context to create new context's for re-connecting the runner.
	runnerCtx context.Context
}

// updateId updates lastStageId and returns the *lucy.StageID value that should be sent
// with this request.
func (mock *ClientRunnerMock) updateId(in *lucy.StageID) *lucy.StageID {
	defer func() {
		mock.lastStageId = in
	}()

	if !in.Eq(mock.lastStageId) {
		return in
	} else {
		return nil
	}
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

	// Exit if we are not looking for a confirmation
	if !update.Confirm {
		return nil
	}

	// Otherwise grab the confirmation.
	_, err = mock.runner.Recv()
	return err
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

// resetStream creates a new runner stream on error.
func (mock *ClientRunnerMock) resetStream(err error) {
	if err == nil {
		return
	} else if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return
	}

	mock.runner.CloseSend()

	mock.runner, err = mock.LucyClient.Runner(context.Background())
	if err != nil {
		panic(fmt.Errorf("erroor re-connecting runner: %w", err))
	}
	mock.lastStageId = nil
}

func (mock *ClientRunnerMock) StartStage(
	ctx context.Context, in *lucy.StartStage, opts ...grpc.CallOption,
) (resp *empty.Empty, err error) {
	defer func() {
		mock.resetStream(err)
	}()
	resp, err = mock.sendUpdate(
		ctx,
		&lucy.RunnerUpdate{
			StageId: mock.updateId(in.StageId),
			Update: &lucy.RunnerUpdate_Start{
				Start: in.Update,
			},
			Confirm: true,
		},
	)
	return resp, err
}

func (mock *ClientRunnerMock) ProgressStage(
	ctx context.Context, in *lucy.ProgressStage, opts ...grpc.CallOption,
) (resp *empty.Empty, err error) {
	defer func() {
		mock.resetStream(err)
	}()
	resp, err = mock.sendUpdate(
		ctx,
		&lucy.RunnerUpdate{
			StageId: mock.updateId(in.StageId),
			Update: &lucy.RunnerUpdate_Progress{
				Progress: in.Update,
			},
			Confirm: true,
		},
	)
	return resp, err
}

func (mock *ClientRunnerMock) CompleteStage(
	ctx context.Context,
	in *lucy.CompleteStage,
	opts ...grpc.CallOption,
) (resp *empty.Empty, err error) {
	defer func() {
		mock.resetStream(err)
	}()
	resp, err = mock.sendUpdate(
		ctx,
		&lucy.RunnerUpdate{
			StageId: mock.updateId(in.StageId),
			Update: &lucy.RunnerUpdate_Complete{
				Complete: in.Update,
			},
			Confirm: true,
		},
	)
	return resp, err
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
		runnerCtx:   context.Background(),
	}
}

func (suite *LucyBatchStreamSuite) TearDownSuite() {
	mock := suite.client.(*ClientRunnerMock)
	err := mock.runner.CloseSend()
	if !suite.NoError(err, "Runner.CloseSend()") {
		suite.FailNow("could not close update stream")
	}
	suite.lucySuite.TearDownSuite()
}

func TestLucyBatchStreamSuite(t *testing.T) {
	suite.Run(t, &LucyBatchStreamSuite{
		LucyBatchUnarySuite: LucyBatchUnarySuite{
			lucySuite: newLucySuite(),
		},
	})
}

type LucyErrorsSuite struct {
	lucySuite

	batchId *cerealMessages.UUID
	jobId   *cerealMessages.UUID
}

func (suite *LucyErrorsSuite) SetupSuite() {
	suite.lucySuite.SetupSuite()

	batch := &lucy.NewBatch{
		Type:        BatchType,
		Name:        "Sort Students",
		Description: "send new students through the sorting hat",
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	created, err := suite.client.CreateBatch(ctx, batch)
	if !suite.NoError(err, "create batch") {
		suite.T().FailNow()
	}
	suite.batchId = created.BatchId

	job := suite.NewJob("Ron Weasley")
	newJobs := &lucy.NewJobs{
		Batch: suite.batchId,
		Jobs:  []*lucy.NewJob{job},
	}

	ctx, cancel = pktesting.New3SecondCtx()
	defer cancel()

	jobsCreated, err := suite.client.CreateJobs(ctx, newJobs)
	if !suite.NoError(err, "create jobs") {
		suite.T().FailNow()
	}

	if !suite.Len(jobsCreated.Ids, 1, "1 id returned") {
		suite.T().FailNow()
	}

	suite.jobId = jobsCreated.Ids[0]
}

func (suite *LucyErrorsSuite) Test0010_GetBatch_NotExists() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.GetBatch(ctx, cerealMessages.MustUUIDRandom())
	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)
	assertErr.Sentinel(pkerr.ErrNotFound, true)
}

func (suite *LucyErrorsSuite) Test0020_GetJob_NotExists() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.GetJob(ctx, cerealMessages.MustUUIDRandom())
	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)
	assertErr.Sentinel(pkerr.ErrNotFound, true)
}

func (suite *LucyErrorsSuite) Test0030_StartJob_NotExists() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	update := &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId:      cerealMessages.MustUUIDRandom(),
			StageIndex: 0,
		},
		Update: &lucy.StartStageUpdate{RunBy: "testworker"},
	}

	_, err := suite.client.StartStage(ctx, update)
	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)
	assertErr.Sentinel(pkerr.ErrNotFound, false)
	assertErr.Message(
		"requested resource not found: no job stage found that matched " +
			"stage_id. make sure the job id is correct, and the stage index is not " +
			"out of bounds",
	)
}

func (suite *LucyErrorsSuite) Test0040_ProgressJob_NotExists() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	update := &lucy.ProgressStage{
		StageId: &lucy.StageID{
			JobId:      cerealMessages.MustUUIDRandom(),
			StageIndex: 0,
		},
		Update: &lucy.ProgressStageUpdate{
			Progress: 0.5,
		},
	}

	_, err := suite.client.ProgressStage(ctx, update)

	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)
	assertErr.Sentinel(pkerr.ErrNotFound, false)
	assertErr.Message(
		"requested resource not found: no job stage found that matched " +
			"stage_id. make sure the job id is correct, and the stage index is not " +
			"out of bounds",
	)
}

func (suite *LucyErrorsSuite) Test0050_CompleteJob_NotExists() {
	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	update := &lucy.CompleteStage{
		StageId: &lucy.StageID{
			JobId:      cerealMessages.MustUUIDRandom(),
			StageIndex: 0,
		},
		Update: &lucy.CompleteStageUpdate{
			Error:      nil,
			ResultData: nil,
		},
	}

	_, err := suite.client.CompleteStage(ctx, update)
	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)
	assertErr.Sentinel(pkerr.ErrNotFound, false)
	assertErr.Message(
		"requested resource not found: no job stage found that matched " +
			"stage_id. make sure the job id is correct, and the stage index is not " +
			"out of bounds",
	)
}

func (suite *LucyErrorsSuite) Test0060_ProgressJob_NotStarted() {
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
	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)

	assertErr.Sentinel(lucy.ErrInvalidStageStatus, false)
	assertErr.Message("job stage was not in correct state for update: cannot" +
		" apply progress update: job stage must be in one of the following states: " +
		"RUNNING")
}

func (suite *LucyErrorsSuite) Test0070_CompleteJob_NotStarted() {
	update := &lucy.CompleteStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.CompleteStageUpdate{
			Error:      nil,
			ResultData: nil,
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.CompleteStage(ctx, update)
	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)

	assertErr.Sentinel(lucy.ErrInvalidStageStatus, false)
	assertErr.Message("job stage was not in correct state for update: cannot" +
		" apply complete update: job stage must be in one of the following states: " +
		"RUNNING")
}

func (suite *LucyErrorsSuite) Test0070_ProgressJob_Completed() {
	// First we need to get the stage into a "completed" state.
	start := &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.StartStageUpdate{RunBy: "testworker"},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.StartStage(ctx, start)
	if !suite.NoError(err, "start stage") {
		suite.T().FailNow()
	}

	complete := &lucy.CompleteStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.CompleteStageUpdate{
			Error:      nil,
			ResultData: nil,
		},
	}

	ctx, cancel = pktesting.New3SecondCtx()
	defer cancel()

	_, err = suite.client.CompleteStage(ctx, complete)
	if !suite.NoError(err, "complete stage") {
		suite.T().FailNow()
	}

	// Now try doing a progress update on it, this should cause an error.
	progress := &lucy.ProgressStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.ProgressStageUpdate{Progress: 0.5},
	}

	ctx, cancel = pktesting.New3SecondCtx()
	defer cancel()

	_, err = suite.client.ProgressStage(ctx, progress)

	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)

	assertErr.Sentinel(lucy.ErrInvalidStageStatus, false)
	assertErr.Message("job stage was not in correct state for update: cannot" +
		" apply progress update: job stage must be in one of the following states: " +
		"RUNNING")
}

func (suite *LucyErrorsSuite) Test0080_CompleteJob_Completed() {
	complete := &lucy.CompleteStage{
		StageId: &lucy.StageID{
			JobId:      suite.jobId,
			StageIndex: 0,
		},
		Update: &lucy.CompleteStageUpdate{
			Error:      nil,
			ResultData: nil,
		},
	}

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	_, err := suite.client.CompleteStage(ctx, complete)

	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)

	assertErr.Sentinel(lucy.ErrInvalidStageStatus, false)
	assertErr.Message("job stage was not in correct state for update: cannot" +
		" apply complete update: job stage must be in one of the following states: " +
		"RUNNING",
	)
}

func (suite *LucyErrorsSuite) Test0090_StartJob_Cancelled() {
	batch := suite.NewBatch([]string{"James Potter"})

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	created, err := suite.client.CreateBatch(ctx, batch)
	if !suite.NoError(err, "create batch") {
		suite.T().FailNow()
	}

	_, err = suite.client.CancelBatches(
		ctx, &lucy.CancelBatches{BatchIds: []*cerealMessages.UUID{created.BatchId}},
	)

	if !suite.NoError(err, "cancel batch") {
		suite.T().FailNow()
	}

	_, err = suite.client.StartStage(ctx, &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId:      created.JobIds[0],
			StageIndex: 0,
		},
		Update: &lucy.StartStageUpdate{RunBy: "testworker"},
	})

	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)
	assertErr.Sentinel(lucy.ErrJobCancelled, false)
	assertErr.Message("job is cancelled: cannot apply start update")
}

func (suite *LucyErrorsSuite) Test0100_UpdateJob_MaxRetries() {
	batch := suite.NewBatch([]string{"James Potter"})

	ctx, cancel := pktesting.New3SecondCtx()
	defer cancel()

	created, err := suite.client.CreateBatch(ctx, batch)
	if !suite.NoError(err, "create batch") {
		suite.T().FailNow()
	}

	// Start the job twice
	for i := 0; i < 2; i++ {
		_, err = suite.client.StartStage(ctx, &lucy.StartStage{
			StageId: &lucy.StageID{
				JobId:      created.JobIds[0],
				StageIndex: 0,
			},
			Update: &lucy.StartStageUpdate{RunBy: "testworker"},
		})
		if !suite.NoErrorf(err, "start job %v", i) {
			suite.T().FailNow()
		}
	}

	_, err = suite.client.StartStage(ctx, &lucy.StartStage{
		StageId: &lucy.StageID{
			JobId:      created.JobIds[0],
			StageIndex: 0,
		},
		Update: &lucy.StartStageUpdate{RunBy: "testworker"},
	})

	assertErr := pktesting.NewAssertAPIErr(suite.T(), err)
	assertErr.Sentinel(lucy.ErrMaxRetriesExceeded, false)
	assertErr.Message(
		"starting job would exceed max retry limit: cannot apply start update",
	)
}

func TestLucyErrorsSuite(t *testing.T) {
	suite.Run(t, &LucyErrorsSuite{
		lucySuite: newLucySuite(),
	})
}

type LucyErrorStreamSuite struct {
	LucyErrorsSuite
}

func (suite *LucyErrorStreamSuite) SetupSuite() {
	suite.LucyErrorsSuite.SetupSuite()

	runner, err := suite.client.Runner(context.Background())
	if !suite.NoError(err, "get runner stream") {
		suite.FailNow("could not get runner stream")
	}

	suite.client = &ClientRunnerMock{
		LucyClient:  suite.client,
		lastStageId: nil,
		runner:      runner,
		runnerLock:  new(sync.Mutex),
		runnerCtx:   context.Background(),
	}
}

func (suite *LucyErrorStreamSuite) TearDownSuite() {
	mock := suite.client.(*ClientRunnerMock)
	err := mock.runner.CloseSend()
	if !suite.NoError(err, "Runner.CloseSend()") {
		suite.FailNow("could not close update stream")
	}
	suite.lucySuite.TearDownSuite()
}

func TestLucyErrorStreamSuite(t *testing.T) {
	suite.Run(t, &LucyErrorStreamSuite{
		LucyErrorsSuite: LucyErrorsSuite{
			lucySuite: newLucySuite(),
		},
	})
}
