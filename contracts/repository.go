package contracts

import (
	"context"

	"github.com/ekefan/marbl/tasks"
)

// TaskRepository is the storage contract both services depend on.
// Any adapter (postgres, sqlite, in-memory mock) must satisfy this.
type TaskRepository interface {
	// Create persists a new task and returns it with the DB-assigned ID.
	Create(ctx context.Context, taskType tasks.TaskType, value tasks.TaskValue) (*tasks.Task, error)

	// UpdateState moves a task to the next state.
	// Returns an error if the task does not exist or the transition is invalid.
	UpdateState(ctx context.Context, id int64, next tasks.TaskState) error

	// GetByID fetches a single task by its primary key.
	GetByID(ctx context.Context, id int64) (*tasks.Task, error)

	// CountByState returns the number of tasks currently in each state.
	CountByState(ctx context.Context) (map[tasks.TaskState]int64, error)
}