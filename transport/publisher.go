package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ekefan/marbl/contracts"
	"github.com/ekefan/marbl/tasks"
)

var _ contracts.TaskPublisher = (*Publisher)(nil)
var _ contracts.QueueDepthChecker = (*Publisher)(nil)

// taskMessage is the wire(payload) format for tasks over RabbitMQ.
type taskMessage struct {
	ID    int64  `json:"id"`
	Type  int    `json:"type"`
	Value int    `json:"value"`
	State string `json:"state"`
}

// Publisher sends tasks to RabbitMQ and confirms broker receipt.
type Publisher struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
	logger    *slog.Logger
}

// PublisherConfig holds the configuration for the RabbitMQ publisher.
type PublisherConfig struct {
	DSN       string // amqp://user:pass@host:port/vhost
	QueueName string
	Logger    *slog.Logger
}

// NewPublisher connects to RabbitMQ, declares the queue, and enables
// publisher confirms so every Publish call waits for broker acknowledgement.
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	conn, err := amqp.Dial(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	// durable queue survives broker restarts
	_, err = ch.QueueDeclare(
		cfg.QueueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare queue %q: %w", cfg.QueueName, err)
	}

	// publisher confirms: broker acks each message individually
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Publisher{
		conn:      conn,
		ch:        ch,
		queueName: cfg.QueueName,
		logger:    logger,
	}, nil
}

// Publish serialises the task and sends it to the broker.
// Blocks until the broker confirms receipt or the context is cancelled.
// Does NOT wait for the consumer to process the task.
func (p *Publisher) Publish(ctx context.Context, task *tasks.Task) error {
	body, err := json.Marshal(taskMessage{
		ID:    task.ID(),
		Type:  int(task.Type()),
		Value: int(task.Value()),
		State: string(task.State()),
	})
	if err != nil {
		return fmt.Errorf("marshal task %d: %w", task.ID(), err)
	}

	// PublishWithDeferredConfirmWithContext returns a confirmation
	// that we wait on — this is the broker ack, not the consumer ack.
	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"", // default exchange — routes directly to queue by name
		p.queueName,
		true,  // mandatory: error if no queue can accept the message
		false, // immediate: not supported in modern RabbitMQ
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // survives broker restart
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish task %d: %w", task.ID(), err)
	}

	// Wait for broker ack — respects context cancellation
	if ok, err := confirmation.WaitContext(ctx); err != nil {
		return fmt.Errorf("confirm task %d: %w", task.ID(), err)
	} else if !ok {
		return fmt.Errorf("broker nacked task %d", task.ID())
	}

	p.logger.Debug("task published",
		slog.Int64("task_id", task.ID()),
		slog.Int("task_type", int(task.Type())),
		slog.Int("task_value", int(task.Value())),
	)

	return nil
}

// QueueDepth returns the number of ready (unacked) messages in the queue.
// Formally used by the producer to enforce max backlog before publishing.
//
// Deprecated as I inferred maxbacklog to mean task already produced in db but not yet processed

func (p *Publisher) QueueDepth(ctx context.Context) (int64, error) {
	q, err := p.ch.QueueDeclarePassive(
		p.queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("inspect queue %q: %w", p.queueName, err)
	}
	return int64(q.Messages), nil
}

// PurgeQueue removes all pending messages from the queue.
// Intended for use in tests to ensure a clean slate between test cases.
func (p *Publisher) PurgeQueue(ctx context.Context) error {
	_, err := p.ch.QueuePurge(p.queueName, false)
	if err != nil {
		return fmt.Errorf("purge queue %q: %w", p.queueName, err)
	}
	return nil
}

// Close shuts down the channel and connection cleanly.
func (p *Publisher) Close() error {
	if err := p.ch.Close(); err != nil {
		return fmt.Errorf("close channel: %w", err)
	}
	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("close connection: %w", err)
	}
	return nil
}
