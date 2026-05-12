package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPublisher_PublishSingleTask(t *testing.T) {
	pub := newPublisher(t, t.Name())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	task := newTask(t, 1, 3, 50)

	if err := pub.Publish(ctx, task); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	purgeQueue(t, pub)
}

func TestPublisher_QueueDepthIncreasesAfterPublish(t *testing.T) {
	pub := newPublisher(t, t.Name())
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
	purgeQueue(t, pub)
}

func TestPublisher_ContextCancelledAbortPublish(t *testing.T) {
	pub := newPublisher(t, t.Name())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before publish

	task := newTask(t, 1, 0, 0)
	err := pub.Publish(ctx, task)
	if err == nil {
		t.Error("expected error publishing with cancelled context, got nil")
	}
	purgeQueue(t, pub)
}

func TestPublisher_PurgeQueue(t *testing.T) {
	pub := newPublisher(t, t.Name())
	ctx := context.Background()

	for i := range []int{1, 2, 3} {
		task := newTask(t, int64(i+1), (i+1)%10, (i+1)*10)
		_ = pub.Publish(ctx, task)
	}

	if err := pub.PurgeQueue(ctx); err != nil {
		t.Fatalf("PurgeQueue() error: %v", err)
	}

	depth, err := pub.QueueDepth(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), depth)
	purgeQueue(t, pub)
}
