package marbl_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ekefan/marbl/contracts"
	"github.com/ekefan/marbl/internal/mocks"
	"github.com/ekefan/marbl/marbl"
	"github.com/ekefan/marbl/tasks"
	"go.uber.org/mock/gomock"
)

func newProducer(repo *mockRepo, pub contracts.TaskPublisher, maxBacklog int64, rate int) *marbl.Producer {
	return marbl.NewProducer(repo, pub, marbl.ProducerConfig{
		MaxBacklog: maxBacklog,
		Rate:       rate,
	})
}

func TestProducer_StopsAtMaxBacklog(t *testing.T) {
	repo := newMockRepo()
	ctrl := gomock.NewController(t)
	pub := mocks.NewMockTaskPublisher(ctrl)

	const maxBacklog int64 = 5

	pub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil).
		Times(int(maxBacklog))

	producer := newProducer(repo, pub, maxBacklog, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := producer.Run(ctx)

	if !errors.Is(err, marbl.ErrMaxBacklogReached) {
		t.Errorf("expected ErrMaxBacklogReached, got %v", err)
	}

	created := repo.created()
	if int64(len(created)) != maxBacklog {
		t.Errorf("expected %d persisted tasks, got %d", maxBacklog, len(created))
	}
}

func TestProducer_PersistsTaskBeforePublishing(t *testing.T) {
	repo := newMockRepo()
	ctrl := gomock.NewController(t)
	pub := mocks.NewMockTaskPublisher(ctrl)

	const maxBacklog int64 = 3

	pub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, task *tasks.Task) error {
			if _, exists := repo.tasks[task.ID()]; !exists {
				t.Error("tasks published before being persisted")
			}
			return nil
		}).
		Times(int(maxBacklog))

	producer := newProducer(repo, pub, maxBacklog, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = producer.Run(ctx)
}

func TestProducer_TasksHaveValidTypeAndValue(t *testing.T) {
	repo := newMockRepo()
	ctrl := gomock.NewController(t)
	pub := mocks.NewMockTaskPublisher(ctrl)

	const maxBacklog int64 = 20

	pub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil).
		Times(int(maxBacklog))

	producer := newProducer(repo, pub, maxBacklog, 100)

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
	ctrl := gomock.NewController(t)
	pub := mocks.NewMockTaskPublisher(ctrl)

	// high backlog so only ctx cancel stops it — unknown number of publishes
	pub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

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
	ctrl := gomock.NewController(t)
	pub := mocks.NewMockTaskPublisher(ctrl)

	const maxBacklog int64 = 3
	publishErr := errors.New("broker unavailable")

	// first call fails, subsequent calls succeed
	// tasks are still persisted on publish failure so backlog still fills up
	gomock.InOrder(
		pub.EXPECT().
			Publish(gomock.Any(), gomock.Any()).
			Return(publishErr).
			Times(1),
		pub.EXPECT().
			Publish(gomock.Any(), gomock.Any()).
			Return(nil).
			Times(int(maxBacklog - 1)),
	)

	producer := newProducer(repo, pub, maxBacklog, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := producer.Run(ctx)

	if !errors.Is(err, marbl.ErrMaxBacklogReached) {
		t.Errorf("expected ErrMaxBacklogReached, got %v", err)
	}
}