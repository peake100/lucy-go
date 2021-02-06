package lucyMessaging

import (
	"context"
	"errors"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cerealMessages"
	"github.com/peake100/lucy-go/lucy"
	"github.com/peake100/lucy-go/lucy/events"
	"github.com/peake100/rogerRabbit-go/roger"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// JobsExchange is the RabbitMQ exchange jobs are published to for workers to receive.
const JobsExchange = "JOBS"
// JobsEventsExchange is the RabbitMQ exchange job events are published to.
const JobsEventsExchange = "JOBS_EVENTS"

// MessageMimeType is the mimetype Lucy messages are written with (application/protobuf)
const MessageMimeType = "application/protobuf"

// EventType is the sting at the end of a an event routing key to indicate what kind
// of event this is.
type EventType string
const (
	// EventTypeCreated is the tag fired when a batch or job is created.
	EventTypeCreated = "CREATED"
	// EventTypeStart is the tag fired when a job or job stage is started.
	EventTypeStart = "START"
	// EventTypeProgress is the tag fired when a job or job stage's progress is
	// updated.
	EventTypeProgress = "PROGRESS"
	// EventTypeComplete is the tag fired when a job or job stage is completed.
	EventTypeComplete = "COMPLETE"
	// EventTypeComplete is the tag fired when a job is cancelled.
	EventTypeCancelled = "CANCELLED"
)

// LucyMessenger holds messaging
type LucyMessenger struct {
	// Conn is the connection we are going to use to send messages.
	Conn *amqp.Connection
	// JobsProducer will used to publish jobs to worker queues. This producer will
	// use a channel put into confirmation mode so we are 100% certain that published
	// jobs made it to the broker.
	JobsProducer *roger.Producer
	// EventsChan. This channel will NOT be put into confirmation mode, we want to
	// prioritize fast responses over the occasional dropped message.
	EventsChan *amqp.Channel
}

// PublishJob publishes a Job to the worker queues.
func (messenger LucyMessenger) PublishJob(
	ctx context.Context,
	job *lucy.Job,
) error {
	message, err := proto.Marshal(job.Id)
	if err != nil {
		return fmt.Errorf("error marshalling job id: %w", err)
	}

	for {
		err := messenger.JobsProducer.Publish(
			ctx,
			JobsExchange,
			job.Type,
			false,
			false,
			amqp.Publishing{
				ContentType:  MessageMimeType,
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now().UTC(),
				AppId:        "Lucy",
				Body:         message,
			},
		)

		if err == nil {
			break
		}

		if errors.Is(err, roger.ErrProducerNack{}) {
			continue
		}

		return fmt.Errorf("error publishing job: %w", err)
	}

	return nil
}

// eventMessage defines the minimum required interface for a Lucy event.
type eventMessage interface {
	proto.Message
	// GetModified returns the modified time of the record.
	GetModified() *timestamppb.Timestamp
}

// publishEvent generically publishes event to the event exchange on routingKey.
func (messenger LucyMessenger) publishEvent(
	routingKey string,
	event eventMessage,
) error {
	message, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshalling event: %w", err)
	}

	err = messenger.EventsChan.Publish(
		JobsEventsExchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  MessageMimeType,
			DeliveryMode: amqp.Transient,
			Timestamp:    event.GetModified().AsTime(),
			AppId:        "Lucy",
			Body:         message,
		},
	)
	if err != nil {
		return fmt.Errorf("error publishing event: %w", err)
	}

	return nil
}

func (messenger LucyMessenger) PublishBatchCreated(event *events.BatchCreated) error {
	routingKey := BatchEventKey(event.Id, EventTypeCreated)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishJobCreated(event *events.JobCreated) error {
	routingKey := JobEventKey(event.Id, EventTypeCreated)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishJobStart(event *events.JobStart) error {
	routingKey := JobEventKey(event.Id, EventTypeStart)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishJobProgress(event *events.JobProgress) error {
	routingKey := JobEventKey(event.Id, EventTypeProgress)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishJobComplete(event *events.JobComplete) error {
	routingKey := JobEventKey(event.Id, EventTypeComplete)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishJobCancel(event *events.JobCancelled) error {
	routingKey := JobEventKey(event.Id, EventTypeCancelled)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishStageStart(event *events.StageStart) error {
	routingKey := StageEventKey(event.Id, EventTypeStart)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishStageProgress(event *events.StageProgress) error {
	routingKey := StageEventKey(event.Id, EventTypeProgress)
	return messenger.publishEvent(routingKey, event)
}

func (messenger LucyMessenger) PublishStageComplete(event *events.StageStart) error {
	routingKey := StageEventKey(event.Id, EventTypeComplete)
	return messenger.publishEvent(routingKey, event)
}

// StageEventKey creates a stage routing event key of a given stage.
func StageEventKey(stageId *events.StageId, eventType string) string {
	return fmt.Sprintf(
		"batch.%v.job.%v.stage.%v.%v",
		stageId.BatchId.MustGoogle().String(),
		stageId.JobId.MustGoogle().String(),
		stageId.StageIndex,
		eventType,
	)
}

// JobEventKey creates a job event routing key for a given job.
func JobEventKey(jobId *events.JobId, eventType string) string {
	return fmt.Sprintf(
		"batch.%v.job.%v.%v",
		jobId.BatchId.MustGoogle().String(),
		jobId.JobId.MustGoogle().String(),
		eventType,
	)
}

// BatchEventKey creates a batch event routing key for a given job.
func BatchEventKey(batchId *cerealMessages.UUID, eventType string) string {
	return fmt.Sprintf(
		"batch.%v.%v",
		batchId.MustGoogle().String(),
		eventType,
	)
}
