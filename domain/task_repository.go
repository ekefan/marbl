package domain

import (
	"context"
)

// TaskRepository is the storage contract both services depend on.
// Any adapter (postgres, sqlite, in-memory mock) must satisfy this.
type TaskRepository interface {
	// Create persists a new task and returns it with the DB-assigned ID.
	Create(ctx context.Context, taskType TaskType, value TaskValue) (*Task, error)

	// UpdateState moves a task to the next state.
	// Returns an error if the task does not exist or the transition is invalid.
	UpdateState(ctx context.Context, id int64, next TaskState) error

	// GetByID fetches a single task by its primary key.
	GetByID(ctx context.Context, id int64) (*Task, error)

	// CountByState returns the number of tasks currently in each state.
	CountByState(ctx context.Context) (map[TaskState]int64, error)

	// SumValueByType returns the total processed value grouped by task type.
	SumValueByType(ctx context.Context) (map[TaskType]int64, error)
}