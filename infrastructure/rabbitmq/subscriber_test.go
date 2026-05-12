package rabbitmq_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ekefan/marbl/comm_channel"
	"github.com/ekefan/marbl/domain"
)

func TestSubscriber_ReceivesSingleTask(t *testing.T) {
	pub := newPublisher(t, t.Name())
	sub := newSubscriber(t, t.Name())
	defer sub.Close()
	defer purgeQueue(t, pub) // pub is closed last, so purge is safe
	ctx := context.Background()

	task := newTask(t, 42, 5, 77)
	if err := pub.Publish(ctx, task); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	received := make(chan *domain.Task, 1)
	handler := comm_channel.TaskHandler(func(ctx context.Context, t *domain.Task) error {
		received <- t
		return nil
	})

	serveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go func() { _ = sub.Serve(serveCtx, handler) }()

	select {
	case got := <-received:
		if got.ID() != task.ID() {
			t.Errorf("id: got %d, want %d", got.ID(), task.ID())
		}
		if got.Type() != task.Type() {
			t.Errorf("type: got %d, want %d", got.Type(), task.Type())
		}
		if got.Value() != task.Value() {
			t.Errorf("value: got %d, want %d", got.Value(), task.Value())
		}
	case <-serveCtx.Done():
		t.Fatal("timed out waiting for task delivery")
	}
}

func TestSubscriber_ReceivesMultipleTasks(t *testing.T) {
	pub := newPublisher(t, t.Name())
	sub := newSubscriber(t, t.Name())
	defer sub.Close()
	defer purgeQueue(t, pub)
	ctx := context.Background()

	const n = 5
	for i := 1; i <= n; i++ {
		task := newTask(t, int64(i), i%10, i*9)
		if err := pub.Publish(ctx, task); err != nil {
			t.Fatalf("Publish() task %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	receivedIDs := make([]int64, 0, n)

	handler := comm_channel.TaskHandler(func(ctx context.Context, task *domain.Task) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, task.ID())
		mu.Unlock()
		return nil
	})

	serveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() { _ = sub.Serve(serveCtx, handler) }()

	deadline := time.After(8 * time.Second)
	for {
		select {
		case <-deadline:
			mu.Lock()
			count := len(receivedIDs)
			mu.Unlock()
			t.Fatalf("timed out: received %d/%d tasks", count, n)
		default:
			mu.Lock()
			count := len(receivedIDs)
			mu.Unlock()
			if count == n {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestSubscriber_NacksAndRequeuesOnHandlerError(t *testing.T) {
	pub := newPublisher(t, t.Name())
	sub := newSubscriber(t, t.Name())
	defer sub.Close()
	defer purgeQueue(t, pub)
	ctx := context.Background()

	task := newTask(t, 1, 0, 10)
	if err := pub.Publish(ctx, task); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	var attempts atomic.Int32
	handler := comm_channel.TaskHandler(func(ctx context.Context, _ *domain.Task) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("simulated failure attempt %d", n)
		}
		return nil
	})

	serveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() { _ = sub.Serve(serveCtx, handler) }()

	deadline := time.After(8 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out: handler only called %d times (expected 3)", attempts.Load())
		default:
			if attempts.Load() >= 3 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestSubscriber_StopsOnContextCancel(t *testing.T) {
	sub := newSubscriber(t, t.Name())
	defer sub.Close()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- sub.Serve(ctx, func(_ context.Context, _ *domain.Task) error {
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve() returned unexpected error on cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve() did not stop after context cancel")
	}
}

func TestRoundtrip_QueueDepthDecreasesAfterConsume(t *testing.T) {
	pub := newPublisher(t, t.Name())
	// purgeQueue uses pub's channel — defer it before sub so pub outlives sub's cleanup
	defer purgeQueue(t, pub)
	ctx := context.Background()

	const n = 4
	for i := 1; i <= n; i++ {
		task := newTask(t, int64(i), i%10, i*5)
		if err := pub.Publish(ctx, task); err != nil {
			t.Fatalf("Publish() task %d: %v", i, err)
		}
		// avoid interleaving QueueDepth's passive QueueDeclare with
		// the confirms channel while publishes are in-flight
	}

	depth, err := pub.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth() after publish error: %v", err)
	}
	if depth != n {
		t.Fatalf("expected depth=%d after publish, got %d", n, depth)
	}

	consumed := make(chan struct{}, n)
	sub := newSubscriber(t, t.Name())
	defer sub.Close()

	handler := comm_channel.TaskHandler(func(_ context.Context, _ *domain.Task) error {
		consumed <- struct{}{}
		return nil
	})

	serveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() { _ = sub.Serve(serveCtx, handler) }()

	for i := 0; i < n; i++ {
		select {
		case <-consumed:
		case <-serveCtx.Done():
			t.Fatal("timed out waiting for consume")
		}
	}

	// brief pause for acks to propagate to broker
	time.Sleep(200 * time.Millisecond)

	depth, err = pub.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth() error: %v", err)
	}
	if depth != 0 {
		t.Errorf("expected depth=0 after consume, got %d", depth)
	}
}