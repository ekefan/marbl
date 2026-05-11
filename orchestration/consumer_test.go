package orchestration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ekefan/marbl/orchestration"
	"github.com/ekefan/marbl/tasks"
	"github.com/stretchr/testify/assert"
)

func newConsumer(repo *mockRepo, rateLimit int) *orchestration.Consumer {
	return orchestration.NewConsumer(repo, orchestration.ConsumerConfig{
		RateLimit: rateLimit,
		RateBurst: rateLimit,
	})
}

func TestConsumer_UpdatesStateToProcessingThenDone(t *testing.T) {
	repo := newMockRepo()
	consumer := newConsumer(repo, 100)

	task, _ := repo.Create(context.Background(), tasks.TaskType(3), tasks.TaskValue(5))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := consumer.Handler()(ctx, task); err != nil {
		t.Fatalf("Handler() error: %v", err)
	}

	updated, _ := repo.GetByID(ctx, task.ID())
	fmt.Println(updated.State())
	if updated.State() != tasks.StateDone {
		t.Errorf("expected state=done, got %q", updated.State())
	}
}

func TestConsumer_SleepsDurationEqualToValue(t *testing.T) {
	repo := newMockRepo()
	consumer := newConsumer(repo, 100)

	// value=50 means 50ms sleep
	task, _ := repo.Create(context.Background(), tasks.TaskType(1), tasks.TaskValue(50))

	ctx := context.Background()
	start := time.Now()
	_ = consumer.Handler()(ctx, task)
	elapsed := time.Since(start)

	// allow generous tolerance for CI variance
	if elapsed < 40*time.Millisecond {
		t.Errorf("expected ~50ms sleep, elapsed only %v", elapsed)
	}
}

func TestConsumer_AggregatesStatsByType(t *testing.T) {
	repo := newMockRepo()
	consumer := newConsumer(repo, 100)
	ctx := context.Background()

	// type 2: values 10 + 20 = 30, count 2
	// type 5: values 15,           count 1
	inputs := []struct {
		typ int
		val int
	}{
		{2, 10},
		{2, 20},
		{5, 15},
	}

	for _, in := range inputs {
		task, _ := repo.Create(ctx, tasks.TaskType(in.typ), tasks.TaskValue(in.val))
		if err := consumer.Handler()(ctx, task); err != nil {
			t.Fatalf("Handler() error: %v", err)
		}
	}

	sumByType, err := repo.SumValueByType(ctx)
	if err != nil {
		t.Fatalf("SumValueByType() error: %v", err)
	}
	assert.Equal(t, int64(30), sumByType[2])
	assert.Equal(t, int64(15), sumByType[5])

	counts, err := repo.CountByState(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), counts[tasks.StateDone])
}

func TestConsumer_RateLimiterThrottlesProcessing(t *testing.T) {
	repo := newMockRepo()
	// rate=2 per second, burst=2
	consumer := newConsumer(repo, 2)
	ctx := context.Background()

	// create 4 tasks with value=0 so sleep is negligible
	taskList := make([]*tasks.Task, 4)
	for i := range taskList {
		task, _ := repo.Create(ctx, tasks.TaskType(i%10), tasks.TaskValue(0))
		taskList[i] = task
	}

	start := time.Now()
	for _, task := range taskList {
		if err := consumer.Handler()(ctx, task); err != nil {
			t.Fatalf("Handler() error: %v", err)
		}
	}
	elapsed := time.Since(start)

	// rate=2/s, burst=2: first 2 are free, next 2 wait ~500ms each
	// so total should be at least ~1 second
	if elapsed < 800*time.Millisecond {
		t.Errorf("rate limiter not throttling: 4 tasks done in %v (expected >= 800ms)", elapsed)
	}
}

func TestConsumer_StopsOnContextCancelDuringSleep(t *testing.T) {
	repo := newMockRepo()
	consumer := newConsumer(repo, 100)

	// value=500 → 500ms sleep, but we cancel after 50ms
	task, _ := repo.Create(context.Background(), tasks.TaskType(0), tasks.TaskValue(99))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := consumer.Handler()(ctx, task)
	if err == nil {
		t.Error("expected error on context cancel during sleep, got nil")
	}
}

func TestConsumer_ConcurrentHandlerCallsAreSafe(t *testing.T) {
	repo := newMockRepo()
	consumer := newConsumer(repo, 1000)
	ctx := context.Background()
	const n = 20
	done := make(chan error, n)
	for i := range n {
		task, _ := repo.Create(
			ctx,
			tasks.TaskType(i%10),
			tasks.TaskValue(1),
		)

		go func(task *tasks.Task) {
			done <- consumer.Handler()(ctx, task)
		}(task)
	}

	for range n {
		if err := <-done; err != nil {
			t.Errorf("concurrent handler error: %v", err)
		}
	}

	counts, err := repo.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState() error: %v", err)
	}

	if counts[tasks.StateDone] != n {
		t.Errorf(
			"expected %d done tasks, got %d",
			n,
			counts[tasks.StateDone],
		)
	}

	sumByType, err := repo.SumValueByType(ctx)
	if err != nil {
		t.Fatalf("SumValueByType() error: %v", err)
	}

	total := int64(0)

	for _, sum := range sumByType {
		total += sum
	}

	// every task had value 1
	if total != n {
		t.Errorf(
			"expected total processed value %d, got %d",
			n,
			total,
		)
	}
}

func TestConsumer_ContinuesProcessingTaskAlreadyInProcessing(t *testing.T) {
	repo := newMockRepo()
	consumer := newConsumer(repo, 100)

	ctx := context.Background()

	task, err := repo.Create(ctx, tasks.TaskType(2), tasks.TaskValue(10))
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	// simulate a previously processing tasks
	err = repo.UpdateState(ctx, task.ID(), tasks.StateProcessing)
	if err != nil {
		t.Fatalf("UpdateState(processing) error: %v", err)
	}
	// now the consumer receives the same task again
	// UpdateState(received -> processing) should fail,
	// triggering continueProcessingTask()
	err = consumer.Handler()(ctx, task)
	if err != nil {
		t.Fatalf("Handler() error: %v", err)
	}
	updated, err := repo.GetByID(ctx, task.ID())
	if err != nil {
		t.Fatalf("GetByID() error: %v", err)
	}

	sumByType, err := repo.SumValueByType(ctx)
	assert.NoError(t, err)
	assert.Equal(t, tasks.StateDone, updated.State(), "expected recovered task state=done, got %q", updated.State())
	assert.Equal(t, int64(10), sumByType[2], "expected type 2 total=10, got %d", sumByType[2])
}
