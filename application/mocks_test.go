package application_test

import (
	"context"
	"sync"

	"github.com/ekefan/marbl/infrastructure/postgres"
	"github.com/ekefan/marbl/domain"
)


type mockRepo struct {
	mu        sync.Mutex
	tasks     map[int64]*domain.Task
	nextID    int64
	createErr error
	updateErr error
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		tasks:  make(map[int64]*domain.Task),
		nextID: 1,
	}
}

func (m *mockRepo) Create(ctx context.Context, taskType domain.TaskType, value domain.TaskValue) (*domain.Task, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.nextID
	m.nextID++

	task, err := domain.NewTask(id, taskType, value)
	if err != nil {
		return nil, err
	}

	m.tasks[id] = task
	return task, nil
}

func (m *mockRepo) UpdateState(ctx context.Context, id int64, next domain.TaskState) error {
	if m.updateErr != nil {
		return m.updateErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[id]
	if !ok {
		return nil
	}

	// mirroring real repo contract behaviou
	// only allowing claiming received -> processing
	switch {
	case task.State() == domain.StateReceived &&
		next == domain.StateProcessing:

	case task.State() == domain.StateProcessing &&
		next == domain.StateDone:

	default:
		return storage.ErrTasksNoUpdate
	}

	return task.Transition(next)
}

func (m *mockRepo) GetByID(ctx context.Context, id int64) (*domain.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasks[id], nil
}

func (m *mockRepo) CountByState(ctx context.Context) (map[domain.TaskState]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	counts := map[domain.TaskState]int64{
		domain.StateReceived:   0,
		domain.StateProcessing: 0,
		domain.StateDone:       0,
	}
	for _, t := range m.tasks {
		counts[t.State()]++
	}
	return counts, nil
}
func (m *mockRepo) SumValueByType(ctx context.Context) (map[domain.TaskType]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[domain.TaskType]int64)

	for _, task := range m.tasks {
		if task.State() != domain.StateDone {
			continue
		}

		result[task.Type()] += int64(task.Value())
	}

	return result, nil
}

func (m *mockRepo) created() []*domain.Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*domain.Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out
}