package marbl_test

import (
	"context"
	"sync"

	"github.com/ekefan/marbl/storage"
	"github.com/ekefan/marbl/tasks"
)


type mockRepo struct {
	mu        sync.Mutex
	tasks     map[int64]*tasks.Task
	nextID    int64
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

	// mirroring real repo contract behaviou
	// only allowing claiming received -> processing
	switch {
	case task.State() == tasks.StateReceived &&
		next == tasks.StateProcessing:

	case task.State() == tasks.StateProcessing &&
		next == tasks.StateDone:

	default:
		return storage.ErrTasksNoUpdate
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
func (m *mockRepo) SumValueByType(ctx context.Context) (map[tasks.TaskType]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[tasks.TaskType]int64)

	for _, task := range m.tasks {
		if task.State() != tasks.StateDone {
			continue
		}

		result[task.Type()] += int64(task.Value())
	}

	return result, nil
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