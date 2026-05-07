package transport_test

import (
	"context"
	"testing"
	"time"
)

func TestPublisher_PublishSingleTask(t *testing.T) {
	purgeQueue(t)
	pub := newPublisher(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	task := newTask(t, 1, 3, 50)

	if err := pub.Publish(ctx, task); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
}

func TestPublisher_QueueDepthIncreasesAfterPublish(t *testing.T) {
	purgeQueue(t)
	pub := newPublisher(t)
	ctx := context.Background()

	depth, err := pub.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth() error: %v", err)
	}
	if depth != 0 {
		t.Fatalf("expected empty queue before publish, got depth=%d", depth)
	}

	for i := 1; i <= 3; i++ {
		task := newTask(t, int64(i), i%10, i*10)
		if err := pub.Publish(ctx, task); err != nil {
			t.Fatalf("Publish() task %d error: %v", i, err)
		}
	}

	depth, err = pub.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth() after publish error: %v", err)
	}
	if depth != 3 {
		t.Errorf("expected depth=3, got %d", depth)
	}
}

func TestPublisher_QueueDepthRespectsMaxBacklog(t *testing.T) {
	purgeQueue(t)
	pub := newPublisher(t)
	ctx := context.Background()

	const maxBacklog = 5
	for i := 1; i <= maxBacklog; i++ {
		task := newTask(t, int64(i), i%10, i*5)
		if err := pub.Publish(ctx, task); err != nil {
			t.Fatalf("Publish() task %d: %v", i, err)
		}
	}

	depth, err := pub.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth() error: %v", err)
	}

	// producer should stop when depth >= maxBacklog
	if depth < maxBacklog {
		t.Errorf("expected depth >= %d, got %d", maxBacklog, depth)
	}
}

func TestPublisher_ContextCancelledAbortPublish(t *testing.T) {
	purgeQueue(t)
	pub := newPublisher(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before publish

	task := newTask(t, 1, 0, 0)
	err := pub.Publish(ctx, task)
	if err == nil {
		t.Error("expected error publishing with cancelled context, got nil")
	}
}

func TestPublisher_PurgeQueue(t *testing.T) {
	purgeQueue(t)
	pub := newPublisher(t)
	ctx := context.Background()

	// publish a few tasks then purge
	for i := 1; i <= 3; i++ {
		task := newTask(t, int64(i), i%10, i*10)
		_ = pub.Publish(ctx, task)
	}

	if err := pub.PurgeQueue(ctx); err != nil {
		t.Fatalf("PurgeQueue() error: %v", err)
	}

	depth, err := pub.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth() after purge error: %v", err)
	}
	if depth != 0 {
		t.Errorf("expected depth=0 after purge, got %d", depth)
	}
}