package orchestration

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/ekefan/marbl/contracts"
	"github.com/ekefan/marbl/tasks"
)

// ProducerConfig holds all runtime configuration for the producer.
type ProducerConfig struct {
	// MaxBacklog is the maximum number of unprocessed messages allowed
	// in the system before the producer stops publishing.
	MaxBacklog int64

	// Rate is how many tasks to produce per second.
	Rate int

	// OnProduce is an optional hook called after each successful publish.
	// Currently being used to increment prometheus counters without importing metrics
	// into the orchestration package.
	OnProduce func()

	Logger *slog.Logger
}

// Producer generates tasks at a configured rate and publishes them to the
// broker. It stops cleanly when the queue backlog reaches MaxBacklog.
type Producer struct {
	repo      contracts.TaskRepository
	publisher contracts.TaskPublisher
	cfg       ProducerConfig
	logger    *slog.Logger
}

// NewProducer wires up the producer with its dependencies.
func NewProducer(
	repo contracts.TaskRepository,
	publisher contracts.TaskPublisher,
	cfg ProducerConfig,
) *Producer {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Producer{
		repo:      repo,
		publisher: publisher,
		cfg:       cfg,
		logger:    logger,
	}
}

// Run starts the production loop. Blocks until:
//   - the queue backlog reaches MaxBacklog (clean stop)
//   - ctx is cancelled (graceful shutdown)
//   - an unrecoverable error occurs
func (p *Producer) Run(ctx context.Context) error {
	if p.cfg.Rate <= 0 {
		p.cfg.Rate = 1
	}
	interval := time.Second / time.Duration(p.cfg.Rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	p.logger.Info("producer started",
		slog.Int("rate_per_second", p.cfg.Rate),
		slog.Int64("max_backlog", p.cfg.MaxBacklog),
	)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("producer stopping", slog.String("reason", ctx.Err().Error()))
			return nil

		case <-ticker.C:
			if err := p.produce(ctx); err != nil {
				return err
			}
		}
	}
}

// produce performs one task generation cycle:
// 1. check backlog — stop if at limit
// 2. generate random type(0-9) & value(0 - 99)
// 3. persist to DB in received state
// 4. publish to broker
func (p *Producer) produce(ctx context.Context) error {
	counts, err := p.repo.CountByState(ctx)
	if err != nil {
		p.logger.Warn("failed_backlog_check", slog.String("error", err.Error()))
		return nil
	}

	currentBacklog := counts[tasks.StateReceived] + counts[tasks.StateProcessing]

	if currentBacklog >= p.cfg.MaxBacklog {
		p.logger.Info("max backlog reached, producer stopping",
			slog.Int64("currentBacklog", currentBacklog),
			slog.Int64("max_backlog", p.cfg.MaxBacklog),
		)
		return ErrMaxBacklogReached
	}

	taskType := tasks.TaskType(rand.IntN(10))   // [0, 9]
	taskValue := tasks.TaskValue(rand.IntN(100)) // [0, 99]

	task, err := p.repo.Create(ctx, taskType, taskValue)
	if err != nil {
		p.logger.Error("failed to persist task",
			slog.String("error", err.Error()),
		)
		return nil
	}

	if err := p.publisher.Publish(ctx, task); err != nil {
		p.logger.Error("failed to publish task",
			slog.Int64("task_id", task.ID()),
			slog.String("error", err.Error()),
		)
		// handle this with traditional outbox pattern....
		return nil
	}

	p.logger.Info("task produced",
		slog.Int64("task_id", task.ID()),
		slog.Int("task_type", int(task.Type())),
		slog.Int("task_value", int(task.Value())),
	)

	if p.cfg.OnProduce != nil {
		p.cfg.OnProduce()
	}

	return nil
}