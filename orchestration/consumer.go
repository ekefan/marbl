package orchestration

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/ekefan/marbl/contracts"
	"github.com/ekefan/marbl/tasks"
)

// ConsumerConfig holds all runtime configuration for the consumer.
type ConsumerConfig struct {
	// RateLimit is the maximum number of tasks processed per second.
	RateLimit int

	// RateBurst is the maximum burst size for the token bucket.
	// Defaults to RateLimit if zero.
	RateBurst int

	// OnProcessing is called when a task moves to processing state.
	OnProcessing func()

	// OnDone is called when a task completes, with its type and value.
	OnDone func(taskType int, value int)

	Logger *slog.Logger
}

// typeStats tracks per-type aggregation under a single RWMutex.
// Both the count and sum are updated together to avoid inconsistency.
type typeStats struct {
	mu    sync.RWMutex
	count map[int]int64 // task type → number of processed tasks
	sum   map[int]int64 // task type → total sum of values
}

func newTypeStats() *typeStats {
	return &typeStats{
		count: make(map[int]int64),
		sum:   make(map[int]int64),
	}
}

func (ts *typeStats) record(taskType int, value int) {
	ts.mu.Lock()
	ts.count[taskType]++
	ts.sum[taskType] += int64(value)
	ts.mu.Unlock()
}

// Snapshot returns a consistent copy of both maps at one point in time.
func (ts *typeStats) Snapshot() (count map[int]int64, sum map[int]int64) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	count = make(map[int]int64, len(ts.count))
	sum = make(map[int]int64, len(ts.sum))
	for k, v := range ts.count {
		count[k] = v
	}
	for k, v := range ts.sum {
		sum[k] = v
	}
	return count, sum
}

// Consumer processes incoming tasks with a rate limiter.
// It updates DB state, records per-type aggregations, and logs each task.
type Consumer struct {
	repo    contracts.TaskRepository
	limiter *rate.Limiter
	stats   *typeStats
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
		stats:   newTypeStats(),
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

		if err := c.repo.UpdateState(ctx, task.ID(), tasks.StateProcessing); err != nil {
			return err
		}

		if c.cfg.OnProcessing != nil {
			c.cfg.OnProcessing()
		}

		select {
		case <-time.After(time.Duration(task.Value()) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := c.repo.UpdateState(ctx, task.ID(), tasks.StateDone); err != nil {
			return err
		}

		// record per-type aggregation
		c.stats.record(int(task.Type()), int(task.Value()))

		if c.cfg.OnDone != nil {
			c.cfg.OnDone(int(task.Type()), int(task.Value()))
		}

		// final log per task as required by the spec
		_, sumByType := c.stats.Snapshot()
		c.logger.Info("task processed",
			slog.Int64("task_id", task.ID()),
			slog.Int("task_type", int(task.Type())),
			slog.Int("task_value", int(task.Value())),
			slog.Int64("type_total_value", sumByType[int(task.Type())]),
		)

		return nil
	}
}

// Stats returns a point-in-time snapshot of per-type aggregations.
// Used by the metrics layer to update prometheus gauges.
func (c *Consumer) Stats() (count map[int]int64, sum map[int]int64) {
	return c.stats.Snapshot()
}