package domain

import (
	"errors"
	"fmt"
	"time"
)

// TaskType is a value between 0 and 9 inclusive.
type TaskType int

const (
	minTaskType TaskType = 0
	maxTaskType TaskType = 9
)

// TaskState represents the lifecycle of a task through the system.
type TaskState string

const (
	StateReceived   TaskState = "received"
	StateProcessing TaskState = "processing"
	StateDone       TaskState = "done"
)

// validTransitions defines the only legal state moves.
// A task may not skip states or move backwards.
var validTransitions = map[TaskState]TaskState{
	StateReceived:   StateProcessing,
	StateProcessing: StateDone,
}

// TaskValue is a value between 0 and 99 inclusive.
type TaskValue int

const (
	minTaskValue TaskValue = 0
	maxTaskValue TaskValue = 99
)

// Task is the central entity of the system.
type Task struct {
	id             int64
	taskType       TaskType
	taskValue      TaskValue
	state          TaskState
	creationTime   time.Time
	lastUpdateTime time.Time
}

func NewTask(id int64, taskType TaskType, value TaskValue) (*Task, error) {
	if err := validateTaskType(taskType); err != nil {
		return nil, err
	}
	if err := validateTaskValue(value); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	return &Task{
		id:             id,
		taskType:       taskType,
		taskValue:      value,
		state:          StateReceived,
		creationTime:   now,
		lastUpdateTime: now,
	}, nil
}

// Transition moves the task to the next valid state.
// Returns an error if the transition is not permitted.
func (t *Task) Transition(next TaskState) error {
	allowed, ok := validTransitions[t.state]
	if !ok {
		return fmt.Errorf("task %d is in terminal state %q: no further transitions allowed", t.id, t.state)
	}
	if allowed != next {
		return fmt.Errorf("task %d: invalid transition %q -> %q (expected %q)", t.id, t.state, next, allowed)
	}

	t.state = next
	t.lastUpdateTime = time.Now().UTC()
	return nil
}

// Reconstitute rebuilds a Task from persisted data
func Reconstitute(id int64, taskType TaskType, value TaskValue, state TaskState, createdAt, updatedAt time.Time) (*Task, error) {
	if err := validateTaskType(taskType); err != nil {
		return nil, err
	}
	if err := validateTaskValue(value); err != nil {
		return nil, err
	}
	if err := validateState(state); err != nil {
		return nil, err
	}

	return &Task{
		id:             id,
		taskType:       taskType,
		taskValue:      value,
		state:          state,
		creationTime:   createdAt.UTC(),
		lastUpdateTime: updatedAt.UTC(),
	}, nil
}

func (t *Task) ID() int64                 { return t.id }
func (t *Task) Type() TaskType            { return t.taskType }
func (t *Task) Value() TaskValue          { return t.taskValue }
func (t *Task) State() TaskState          { return t.state }
func (t *Task) CreationTime() time.Time   { return t.creationTime }
func (t *Task) LastUpdateTime() time.Time { return t.lastUpdateTime }

func (t *Task) String() string {
	return fmt.Sprintf("Task{id=%d type=%d value=%d state=%s}", t.id, t.taskType, t.taskValue, t.state)
}

func validateTaskType(tt TaskType) error {
	if tt < minTaskType || tt > maxTaskType {
		return fmt.Errorf("task type %d out of range [%d, %d]", tt, minTaskType, maxTaskType)
	}
	return nil
}

func validateTaskValue(tv TaskValue) error {
	if tv < minTaskValue || tv > maxTaskValue {
		return fmt.Errorf("task value %d out of range [%d, %d]", tv, minTaskValue, maxTaskValue)
	}
	return nil
}

func validateState(s TaskState) error {
	switch s {
	case StateReceived, StateProcessing, StateDone:
		return nil
	default:
		return fmt.Errorf("unknown task state %q", s)
	}
}

var (
	ErrInvalidTaskType   = errors.New("invalid task type")
	ErrInvalidTaskValue  = errors.New("invalid task value")
	ErrInvalidTransition = errors.New("invalid state transition")
)
