package orchestration_test

import (
	"context"
	"testing"
	"time"

	"github.com/ekefan/marbl/orchestration"
	"github.com/ekefan/marbl/tasks"
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

	count, sum := consumer.Stats()

	if count[2] != 2 {
		t.Errorf("type 2 count: got %d, want 2", count[2])
	}
	if sum[2] != 30 {
		t.Errorf("type 2 sum: got %d, want 30", sum[2])
	}
	if count[5] != 1 {
		t.Errorf("type 5 count: got %d, want 1", count[5])
	}
	if sum[5] != 15 {
		t.Errorf("type 5 sum: got %d, want 15", sum[5])
	}
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

	for i := 0; i < n; i++ {
		task, _ := repo.Create(ctx, tasks.TaskType(i%10), tasks.TaskValue(0))
		go func(task *tasks.Task) {
			done <- consumer.Handler()(ctx, task)
		}(task)
	}

	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Errorf("concurrent handler error: %v", err)
		}
	}

	count, _ := consumer.Stats()
	total := int64(0)
	for _, c := range count {
		total += c
	}
	if total != n {
		t.Errorf("expected %d total processed, got %d", n, total)
	}
}