package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ekefan/marbl/contracts"
	"github.com/ekefan/marbl/tasks"
)

// compile-time contract check
var _ contracts.TaskSubscriber = (*Subscriber)(nil)

// unrecoverableError wraps errors that should NOT be requeued —
// malformed JSON, invalid domain data. Requeuing these would loop forever.
type unrecoverableError struct{ cause error }

func (e *unrecoverableError) Error() string { return e.cause.Error() }
func (e *unrecoverableError) Unwrap() error { return e.cause }

func isUnrecoverable(err error) bool {
	var u *unrecoverableError
	return errors.As(err, &u)
}

// Subscriber receives tasks from RabbitMQ and dispatches them to a handler.
// It uses manual acks — a message is only removed from the queue after
// the handler returns nil. On handler error the message is nacked and requeued.
type Subscriber struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
	prefetch  int
	logger    *slog.Logger
}

// SubscriberConfig holds the configuration for the RabbitMQ subscriber.
type SubscriberConfig struct {
	DSN       string
	QueueName string
	// Prefetch is the max number of unacked messages the broker will push
	// to this consumer at once. Tune this alongside the rate limiter.
	// A value of 1 means strict sequential processing.
	Prefetch int
	Logger   *slog.Logger
}

// NewSubscriber connects to RabbitMQ and declares the queue.
func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	conn, err := amqp.Dial(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		cfg.QueueName,
		true,  // durable — must match publisher declaration
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

	prefetch := cfg.Prefetch
	if prefetch <= 0 {
		prefetch = 10
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("set qos prefetch: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Subscriber{
		conn:      conn,
		ch:        ch,
		queueName: cfg.QueueName,
		prefetch:  prefetch,
		logger:    logger,
	}, nil
}

// Serve starts consuming from the queue and dispatches each delivery
// to the handler. Blocks until ctx is cancelled or a fatal broker error.
//
// Ack/Nack behaviour — all decisions live here, nowhere else:
//   - handler returns nil          → ack,  message removed from queue
//   - handler returns error        → nack, requeue=true  (transient, retry)
//   - bad JSON / bad domain data   → nack, requeue=false (permanent, drop)
//   - ctx cancelled                → stop, unacked messages redelivered by broker
func (s *Subscriber) Serve(ctx context.Context, handler contracts.TaskHandler) error {
	deliveries, err := s.ch.Consume(
		s.queueName,
		"",    // consumer tag — broker generates one
		false, // auto-ack disabled — we ack manually after handler
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consuming %q: %w", s.queueName, err)
	}

	s.logger.Info("subscriber ready",
		slog.String("queue", s.queueName),
		slog.Int("prefetch", s.prefetch),
	)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("subscriber stopping", slog.String("reason", ctx.Err().Error()))
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed unexpectedly")
			}

			err := s.handle(ctx, delivery, handler)
			if err == nil {
				_ = delivery.Ack(false)
				continue
			}

			s.logger.Error("handle delivery failed",
				slog.String("error", err.Error()),
				slog.Uint64("delivery_tag", delivery.DeliveryTag),
			)

			// unrecoverable = bad message, drop it (requeue=false)
			// recoverable  = handler error, requeue for retry (requeue=true)
			_ = delivery.Nack(false, !isUnrecoverable(err))
		}
	}
}

// handle deserialises one delivery and calls the handler.
// Never acks or nacks — that is exclusively Serve()'s responsibility.
func (s *Subscriber) handle(ctx context.Context, delivery amqp.Delivery, handler contracts.TaskHandler) error {
	var msg taskMessage
	if err := json.Unmarshal(delivery.Body, &msg); err != nil {
		return &unrecoverableError{fmt.Errorf("unmarshal delivery: %w", err)}
	}

	task, err := tasks.Reconstitute(
		msg.ID,
		tasks.TaskType(msg.Type),
		tasks.TaskValue(msg.Value),
		tasks.TaskState(msg.State),
		delivery.Timestamp.UTC(),
		delivery.Timestamp.UTC(),
	)
	if err != nil {
		return &unrecoverableError{fmt.Errorf("reconstitute task %d: %w", msg.ID, err)}
	}

	s.logger.Debug("task received",
		slog.Int64("task_id", task.ID()),
		slog.Int("task_type", int(task.Type())),
		slog.Int("task_value", int(task.Value())),
	)

	if err := handler(ctx, task); err != nil {
		// recoverable — Serve() will nack with requeue=true
		return fmt.Errorf("handler task %d: %w", task.ID(), err)
	}

	return nil
}

// Close stops the channel and connection cleanly.
func (s *Subscriber) Close() error {
	if err := s.ch.Close(); err != nil {
		return fmt.Errorf("close channel: %w", err)
	}
	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("close connection: %w", err)
	}
	return nil
}