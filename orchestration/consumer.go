package orchestration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/time/rate"

	"github.com/ekefan/marbl/contracts"
	"github.com/ekefan/marbl/storage"
	"github.com/ekefan/marbl/tasks"
)

// ConsumerConfig holds all runtime configuration for the consumer.
type ConsumerConfig struct {
	// RateLimit is the maximum number of tasks processed per second.
	RateLimit int

	// RateBurst is the maximum burst size for the token bucket. For google's Limiter
	// Based on take description, rateburst is equal to ratelimit...
	RateBurst int

	// OnProcessing is called when a task moves to processing state.
	OnProcessing func()

	// OnDone is called when a task completes, with its type and value.
	OnDone func(taskType int, value int)

	Logger *slog.Logger
}

// Consumer processes incoming tasks with a rate limiter.
// It updates DB state, records per-type aggregations, and logs each task.
type Consumer struct {
	repo    contracts.TaskRepository
	limiter *rate.Limiter
	cfg     ConsumerConfig
	logger  *slog.Logger
}

// NewConsumer wires up the consumer with its dependencies.
func NewConsumer(repo contracts.TaskRepository, cfg ConsumerConfig) *Consumer {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	if cfg.RateLimit <= 0 {
		cfg.RateLimit = 10
	}

	burst := cfg.RateBurst
	if burst <= 0 {
		burst = cfg.RateLimit
	}

	return &Consumer{
		repo:    repo,
		limiter: rate.NewLimiter(rate.Limit(cfg.RateLimit), burst),
		cfg:     cfg,
		logger:  logger,
	}
}

// Handler returns a contracts.TaskHandler that the subscriber calls per message.
func (c *Consumer) Handler() contracts.TaskHandler {
	return func(ctx context.Context, task *tasks.Task) error {
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		claimed := false
		if err := c.repo.UpdateState(ctx, task.ID(), tasks.StateProcessing); err != nil {
			if errors.Is(err, storage.ErrTasksNoUpdate) {
				return c.continueProcessingTask(ctx, task)
			}
			return err
		}
		claimed = true
		if claimed && c.cfg.OnProcessing != nil {
			c.cfg.OnProcessing()
		}

		return c.processTask(ctx, task)
	}
}

func (c *Consumer) processTask(ctx context.Context, task *tasks.Task) error {
	select {
	case <-time.After(
		time.Duration(task.Value()) * time.Millisecond,
	):
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.repo.UpdateState(ctx, task.ID(), tasks.StateDone); err != nil {
		return err
	}
	if c.cfg.OnDone != nil {
		c.cfg.OnDone(
			int(task.Type()),
			int(task.Value()),
		)
	}
	sumByType, err := c.repo.SumValueByType(ctx)
	if err != nil {
		return err
	}

	c.logger.Info("task processed",
		slog.Int64("task_id", task.ID()),
		slog.Int("task_type", int(task.Type())),
		slog.Int("task_value", int(task.Value())),
		slog.Int64(
			"type_total_value",
			sumByType[task.Type()],
		),
	)

	return nil
}

func (c *Consumer) continueProcessingTask(ctx context.Context, task *tasks.Task) error {
	current, err := c.repo.GetByID(ctx, task.ID())
	if err != nil {
		return err
	}

	switch current.State() {

	case tasks.StateDone:
		return nil

	case tasks.StateProcessing:
		c.logger.Warn(
			"recovering processing task",
			slog.Int64("task_id", task.ID()),
		)

		return c.processTask(ctx, task)

	default:
		return fmt.Errorf(
			"task %d in unexpected state %q",
			task.ID(),
			current.State(),
		)
	}
}