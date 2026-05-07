package orchestration_test

import (
	"context"
	"sync"

	"github.com/ekefan/marbl/tasks"
)

// --- mock repository ---

type mockRepo struct {
	mu      sync.Mutex
	tasks   map[int64]*tasks.Task
	nextID  int64
	createErr error
	updateErr error
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		tasks:  make(map[int64]*tasks.Task),
		nextID: 1,
	}
}

func (m *mockRepo) Create(ctx context.Context, taskType tasks.TaskType, value tasks.TaskValue) (*tasks.Task, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.nextID
	m.nextID++

	task, err := tasks.New(id, taskType, value)
	if err != nil {
		return nil, err
	}

	m.tasks[id] = task
	return task, nil
}

func (m *mockRepo) UpdateState(ctx context.Context, id int64, next tasks.TaskState) error {
	if m.updateErr != nil {
		return m.updateErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[id]
	if !ok {
		return nil
	}

	return task.Transition(next)
}

func (m *mockRepo) GetByID(ctx context.Context, id int64) (*tasks.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasks[id], nil
}

func (m *mockRepo) CountByState(ctx context.Context) (map[tasks.TaskState]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	counts := map[tasks.TaskState]int64{
		tasks.StateReceived:   0,
		tasks.StateProcessing: 0,
		tasks.StateDone:       0,
	}
	for _, t := range m.tasks {
		counts[t.State()]++
	}
	return counts, nil
}

func (m *mockRepo) created() []*tasks.Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*tasks.Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out
}

// --- mock publisher ---

type mockPublisher struct {
	mu        sync.Mutex
	published []*tasks.Task
	depth     int64
	publishErr error
	depthErr   error
}

func (m *mockPublisher) Publish(ctx context.Context, task *tasks.Task) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.mu.Lock()
	m.published = append(m.published, task)
	m.depth++
	m.mu.Unlock()
	return nil
}

func (m *mockPublisher) QueueDepth(ctx context.Context) (int64, error) {
	if m.depthErr != nil {
		return 0, m.depthErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.depth, nil
}

func (m *mockPublisher) Close() error { return nil }

func (m *mockPublisher) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.published)
}