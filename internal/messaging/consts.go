package messaging

// EnvKeyRabbitURI is the env var key that the rabbitMQ URI will be pulled from for
// connections.
const EnvKeyRabbitURI = "LUCY_RABBITMQ_URI"

// JobsExchange is the RabbitMQ exchange jobs are published to for workers to receive.
const JobsExchange = "JOBS"

// JobsEventsExchange is the RabbitMQ exchange job events are published to.
const JobsEventsExchange = "JOBS_EVENTS"

// MessageMimeType is the mimetype Lucy messages are written with (application/protobuf)
const MessageMimeType = "application/protobuf"
