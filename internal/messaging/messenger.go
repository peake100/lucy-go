package messaging

import (
	"context"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/lucy-go/pkg/lucy/events"
	"github.com/peake100/rogerRabbit-go/amqp"
	"github.com/peake100/rogerRabbit-go/roger"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"sync"
	"time"
)

// eventProto defines the minimum required interface for a Lucy event.
type eventProto interface {
	proto.Message
	// GetModified returns the modified time of the record.
	GetModified() *timestamppb.Timestamp
}

// queuedEvent is an event that needs to be sent to the broker.
type queuedEvent struct {
	// RoutingKey is the routing key for the event.
	RoutingKey string
	// Proto is the event message itself.
	Proto eventProto
	// Logger is the logger to use for this event.
	Logger zerolog.Logger
}

// LucyMessenger holds messaging
type LucyMessenger struct {
	// conn is the connection we are going to use to communicate with the RabbitMQ
	// broker.
	conn *amqp.Connection
	// jobsChan is the channel jobs will be published over.
	jobsChan *amqp.Channel
	// jobsProducer will used to publish jobs to worker queues. This producer will
	// use a channel put into confirmation mode so we are 100% certain that published
	// jobs made it to the broker.
	jobsProducer *roger.Producer
	// Holds the job types we have declared queues for.
	jobQueuesDeclared *sync.Map

	// internalEventQueue holds a queue of events to be sent to the broker.
	internalEventQueue chan queuedEvent
	// internalEventsSent is closed when all internal events are done being sent.
	internalEventsSent chan struct{}
	// eventsChan. This channel will NOT be put into confirmation mode, we want to
	// prioritize fast responses over the occasional dropped message.
	eventsChan *amqp.Channel
}

// Setup sets up the broker routing topology that lucy needs.
func (messenger *LucyMessenger) Setup(
	ctx context.Context, logger zerolog.Logger,
) (err error) {
	RabbitURI := os.Getenv(EnvKeyRabbitURI)
	if RabbitURI == "" {
		RabbitURI = "amqp://localhost:5672"
	}

	config := amqp.DefaultConfig()
	config.Logger = logger.With().Str("RESOURCE", "RabbitMQ").Logger()

	messenger.conn, err = amqp.DialConfigCtx(ctx, RabbitURI, *config)
	if err != nil {
		return fmt.Errorf("error connecting to rabbitMQ: %w", err)
	}

	// Get a dedicated channel for our jobs producer.
	jobsChan, err := messenger.conn.Channel()
	if err != nil {
		return fmt.Errorf("error getting jobs channel: %w", err)
	}
	messenger.jobsChan = jobsChan

	jobsChannel, err := messenger.conn.Channel()
	if err != nil {
		return fmt.Errorf("error getting events jobs channel: %w", err)
	}

	err = jobsChannel.ExchangeDeclare(
		JobsExchange,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	)

	messenger.jobsProducer = roger.NewProducer(jobsChannel, roger.NewProducerOpts())

	messenger.eventsChan, err = messenger.conn.Channel()
	if err != nil {
		return fmt.Errorf(
			"error getting events channel: %w", err,
		)
	}

	err = messenger.eventsChan.ExchangeDeclare(
		JobsEventsExchange,
		amqp.ExchangeTopic,
		false,
		false,
		false,
		false,
		nil,
	)

	return nil
}

// Run runs the messenger until ctx is cancelled.
func (messenger LucyMessenger) Run(ctx context.Context) (err error) {
	// Close te internal event queue on the way out.
	defer func() {
		<-messenger.internalEventsSent
	}()
	defer close(messenger.internalEventQueue)

	go messenger.processQueuedEvents()

	producerErr := make(chan error)

	// Close the rabbit channel / connection
	defer messenger.conn.Close()
	defer messenger.eventsChan.Close()
	// Capture any producer errors.
	defer func() {
		err = <-producerErr
	}()
	// Start the shutdown of the messenger.
	defer messenger.jobsProducer.StartShutdown()

	go func() {
		producerErr <- messenger.jobsProducer.Run()
	}()

	// Block until the context cancels
	<-ctx.Done()
	return err
}

// processQueuedEvents takes in queued events and sends them to the broker. We want to
// do this in it's own goroutine so we preserve event order as much as possible.
func (messenger LucyMessenger) processQueuedEvents() {
	// On the way out, signal we have exited.
	defer close(messenger.internalEventsSent)

	// Iterate over our internal queue until it has dried up.
	for thisEvent := range messenger.internalEventQueue {
		messenger.publishEvent(thisEvent)
	}
}

// publishEvent generically publishes event to the event exchange on routingKey.
// Because events are published in the background, no error is returned, instead, it is
// logged to the logger.
func (messenger LucyMessenger) publishEvent(event queuedEvent) {
	var err error
	defer func() {
		if err != nil {
			event.Logger.Error().
				Err(err).
				Str("ROUTING_KEY", event.RoutingKey).
				Msg("error publishing event")
		}
	}()

	message, err := proto.Marshal(event.Proto)
	if err != nil {
		err = fmt.Errorf("error marshalling event: %w", err)
		return
	}

	err = messenger.eventsChan.Publish(
		JobsEventsExchange,
		event.RoutingKey,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				// Event feeds are meant for programs to get a running list of current
				// events. Messages will expire after 5 seconds.
				"x-message-ttl": int64(5 * time.Second / time.Millisecond),
			},
			ContentType: MessageMimeType,
			// This is a transient message, it does not need to be saved to disk. This
			// is faster but means some messages might be dropped.
			DeliveryMode: amqp.Transient,
			// The message timestamp will be the record's modified timestamp, that way
			// users can the message timestamp to order update messages that may come
			// out of order.
			Timestamp: event.Proto.GetModified().AsTime().UTC(),
			// The name of the service will be the AppId.
			AppId: "Lucy",
			// Our marshalled protobuf will be the body.
			Body: message,
		},
	)
	if err != nil {
		err = fmt.Errorf("error publishing event: %w", err)
	}
}

// QueueJob queues a Job to be published on the worker queue. The returned
// roger.Publication's Wait method must be called to confirm publication with the
// broker.
func (messenger LucyMessenger) QueueJob(
	ctx context.Context,
	jobId *cereal.UUID,
	jobType string,
) (*roger.Publication, error) {
	// Marshal the job id message.
	message, err := proto.Marshal(jobId)
	if err != nil {
		return nil, fmt.Errorf("error marshalling job id: %w", err)
	}

	publication, err := messenger.jobsProducer.QueueForPublication(
		ctx,
		JobsExchange,
		jobType,
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
	if err != nil {
		return nil, fmt.Errorf("error queuing job message")
	}

	return publication, nil
}

func (messenger LucyMessenger) DeclareJobQueue(jobType string) error {
	type queueLock struct {
		Lock    sync.Mutex
		Created bool
	}

	lock := new(queueLock)
	lockVal, loaded := messenger.jobQueuesDeclared.LoadOrStore(jobType, lock)
	if loaded {
		lock = lockVal.(*queueLock)
	}

	// Check if the queue has been created and return if it has so we don't have to
	// acquire the lock.
	if lock.Created {
		return nil
	}

	// Acquire the lock.
	lock.Lock.Lock()
	defer lock.Lock.Unlock()

	// Check again, someone else may have created the queue while we were acquiring the
	// lock.
	if lock.Created {
		return nil
	}

	// Declare the queue
	queue, err := messenger.jobsChan.QueueDeclare(
		WorkerQueueName(jobType),
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("error declaring queue")
	}

	// Bind the queue
	err = messenger.jobsChan.QueueBind(
		queue.Name,
		jobType,
		JobsExchange,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error binding queue")
	}

	// Set the created value to true. This is a pointer so we don't neeed to store it
	// in the sync map again.
	lock.Created = true
	return nil
}

// queueEvent puts an event into the internal queue to be sent to the broker, only
// blocking if a large number of events are already queued.
func (messenger LucyMessenger) queueEvent(
	routingKey string,
	event eventProto,
	rpcLogger zerolog.Logger,
) {
	messenger.internalEventQueue <- queuedEvent{
		RoutingKey: routingKey,
		Proto:      event,
		Logger:     rpcLogger,
	}
}

func (messenger LucyMessenger) QueueBatchCreated(
	event *events.BatchCreated, rpcLogger zerolog.Logger,
) {
	routingKey := BatchEventKey(event.Id, EventTypeCreated)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueBatchUpdated(
	event *events.BatchUpdated, rpcLogger zerolog.Logger,
) {
	routingKey := BatchEventKey(event.Id, EventTypeUpdated)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueJobCreated(
	event *events.JobCreated, rpcLogger zerolog.Logger,
) {
	routingKey := JobEventKey(event.Id, EventTypeCreated)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueJobStart(
	event *events.JobStart, rpcLogger zerolog.Logger,
) {
	routingKey := JobEventKey(event.Id, EventTypeStart)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueJobProgress(
	event *events.JobProgress, rpcLogger zerolog.Logger,
) {
	routingKey := JobEventKey(event.Id, EventTypeProgress)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueJobComplete(
	event *events.JobComplete, rpcLogger zerolog.Logger,
) {
	routingKey := JobEventKey(event.Id, EventTypeComplete)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueJobCancel(
	event *events.JobCancelled, rpcLogger zerolog.Logger,
) {
	routingKey := JobEventKey(event.Id, EventTypeCancelled)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueStageStart(
	event *events.StageStart, rpcLogger zerolog.Logger,
) {
	routingKey := StageEventKey(event.Id, EventTypeStart)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueStageProgress(
	event *events.StageProgress, rpcLogger zerolog.Logger,
) {
	routingKey := StageEventKey(event.Id, EventTypeProgress)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func (messenger LucyMessenger) QueueStageComplete(
	event *events.StageComplete, rpcLogger zerolog.Logger,
) {
	routingKey := StageEventKey(event.Id, EventTypeComplete)
	messenger.queueEvent(routingKey, event, rpcLogger)
}

func NewMessenger() LucyMessenger {
	return LucyMessenger{
		jobQueuesDeclared:  new(sync.Map),
		internalEventQueue: make(chan queuedEvent, 1024),
		internalEventsSent: make(chan struct{}),
	}
}
