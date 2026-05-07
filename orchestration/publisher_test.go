package orchestration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ekefan/marbl/orchestration"
)

func newProducer(repo *mockRepo, pub *mockPublisher, maxBacklog int64, rate int) *orchestration.Producer {
	return orchestration.NewProducer(repo, pub, pub, orchestration.ProducerConfig{
		MaxBacklog: maxBacklog,
		Rate:       rate,
	})
}

func TestProducer_StopsAtMaxBacklog(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}

	const maxBacklog = 5
	producer := newProducer(repo, pub, maxBacklog, 100) // fast rate so test is quick

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := producer.Run(ctx)

	if !errors.Is(err, orchestration.ErrMaxBacklogReached) {
		t.Errorf("expected ErrMaxBacklogReached, got %v", err)
	}
	if int64(pub.count()) != maxBacklog {
		t.Errorf("expected %d published tasks, got %d", maxBacklog, pub.count())
	}
}

func TestProducer_PersistsTaskBeforePublishing(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}

	producer := newProducer(repo, pub, 3, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = producer.Run(ctx)

	created := repo.created()
	if len(created) != pub.count() {
		t.Errorf("DB has %d tasks but %d were published — mismatch", len(created), pub.count())
	}
}

func TestProducer_TasksHaveValidTypeAndValue(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}

	producer := newProducer(repo, pub, 20, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = producer.Run(ctx)

	for _, task := range repo.created() {
		if task.Type() < 0 || task.Type() > 9 {
			t.Errorf("task %d has out-of-range type: %d", task.ID(), task.Type())
		}
		if task.Value() < 0 || task.Value() > 99 {
			t.Errorf("task %d has out-of-range value: %d", task.ID(), task.Value())
		}
	}
}

func TestProducer_StopsOnContextCancel(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}

	// high backlog so it won't hit the limit — only ctx cancel stops it
	producer := newProducer(repo, pub, 10000, 5)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- producer.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected nil on ctx cancel, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("producer did not stop after context cancel")
	}
}

func TestProducer_ContinuesOnPublishError(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}

	// fail first 2 publishes then succeed
	callCount := 0
	pub.publishErr = nil
	originalPublish := pub

	_ = originalPublish // just using pub directly below

	producer := newProducer(repo, pub, 3, 100)

	// inject publish error for first 2 calls
	pub.publishErr = errors.New("broker unavailable")
	go func() {
		time.Sleep(50 * time.Millisecond)
		pub.mu.Lock()
		pub.publishErr = nil
		pub.mu.Unlock()
		_ = callCount
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := producer.Run(ctx)

	// should still reach max backlog eventually, not crash
	if !errors.Is(err, orchestration.ErrMaxBacklogReached) {
		t.Errorf("expected ErrMaxBacklogReached, got %v", err)
	}
}

func TestProducer_DepthErrorIsNonFatal(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}

	// error on first depth check, then clear it
	pub.depthErr = errors.New("broker unreachable")
	go func() {
		time.Sleep(30 * time.Millisecond)
		pub.mu.Lock()
		pub.depthErr = nil
		pub.mu.Unlock()
	}()

	producer := newProducer(repo, pub, 3, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := producer.Run(ctx)
	if !errors.Is(err, orchestration.ErrMaxBacklogReached) {
		t.Errorf("expected ErrMaxBacklogReached after depth error clears, got %v", err)
	}
}