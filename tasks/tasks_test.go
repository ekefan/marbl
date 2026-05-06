package tasks_test

import (
	"testing"
	"time"

	"github.com/ekefan/marbl/tasks"
)

// --- Construction ---

func TestNew_ValidInputs(t *testing.T) {
	task, err := tasks.New(1, tasks.TaskType(3), tasks.TaskValue(50))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if task.ID() != 1 {
		t.Errorf("expected id=1, got %d", task.ID())
	}
	if task.Type() != tasks.TaskType(3) {
		t.Errorf("expected type=3, got %d", task.Type())
	}
	if task.Value() != tasks.TaskValue(50) {
		t.Errorf("expected value=50, got %d", task.Value())
	}
	if task.State() != tasks.StateReceived {
		t.Errorf("expected state=received, got %q", task.State())
	}
}

func TestNew_BoundaryTaskTypes(t *testing.T) {
	cases := []struct {
		name     string
		taskType tasks.TaskType
		wantErr  bool
	}{
		{"min valid", tasks.TaskType(0), false},
		{"max valid", tasks.TaskType(9), false},
		{"below min", tasks.TaskType(-1), true},
		{"above max", tasks.TaskType(10), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tasks.New(1, tc.taskType, tasks.TaskValue(50))
			if (err != nil) != tc.wantErr {
				t.Errorf("New() error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestNew_BoundaryTaskValues(t *testing.T) {
	cases := []struct {
		name      string
		taskValue tasks.TaskValue
		wantErr   bool
	}{
		{"min valid", tasks.TaskValue(0), false},
		{"max valid", tasks.TaskValue(99), false},
		{"below min", tasks.TaskValue(-1), true},
		{"above max", tasks.TaskValue(100), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tasks.New(1, tasks.TaskType(5), tc.taskValue)
			if (err != nil) != tc.wantErr {
				t.Errorf("New() error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestNew_TimestampsSetOnCreation(t *testing.T) {
	before := time.Now().UTC()
	task, err := tasks.New(1, tasks.TaskType(0), tasks.TaskValue(0))
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if task.CreationTime().Before(before) || task.CreationTime().After(after) {
		t.Errorf("CreationTime %v not within expected range [%v, %v]", task.CreationTime(), before, after)
	}
	if task.LastUpdateTime().Before(before) || task.LastUpdateTime().After(after) {
		t.Errorf("LastUpdateTime %v not within expected range", task.LastUpdateTime())
	}
}

// --- State Transitions ---

func TestTransition_HappyPath(t *testing.T) {
	task, _ := tasks.New(1, tasks.TaskType(0), tasks.TaskValue(10))

	if err := task.Transition(tasks.StateProcessing); err != nil {
		t.Fatalf("received -> processing failed: %v", err)
	}
	if task.State() != tasks.StateProcessing {
		t.Errorf("expected processing, got %q", task.State())
	}

	if err := task.Transition(tasks.StateDone); err != nil {
		t.Fatalf("processing -> done failed: %v", err)
	}
	if task.State() != tasks.StateDone {
		t.Errorf("expected done, got %q", task.State())
	}
}

func TestTransition_SkipStateNotAllowed(t *testing.T) {
	task, _ := tasks.New(1, tasks.TaskType(0), tasks.TaskValue(10))

	// try jumping directly received -> done
	err := task.Transition(tasks.StateDone)
	if err == nil {
		t.Error("expected error skipping processing state, got nil")
	}
}

func TestTransition_BackwardsNotAllowed(t *testing.T) {
	task, _ := tasks.New(1, tasks.TaskType(0), tasks.TaskValue(10))
	_ = task.Transition(tasks.StateProcessing)

	// try going back to received
	err := task.Transition(tasks.StateReceived)
	if err == nil {
		t.Error("expected error transitioning backwards, got nil")
	}
}

func TestTransition_TerminalStateBlocked(t *testing.T) {
	task, _ := tasks.New(1, tasks.TaskType(0), tasks.TaskValue(10))
	_ = task.Transition(tasks.StateProcessing)
	_ = task.Transition(tasks.StateDone)

	// done is terminal — no further moves
	err := task.Transition(tasks.StateDone)
	if err == nil {
		t.Error("expected error transitioning from terminal state, got nil")
	}
}

func TestTransition_UpdatesLastUpdateTime(t *testing.T) {
	task, _ := tasks.New(1, tasks.TaskType(0), tasks.TaskValue(10))
	before := task.LastUpdateTime()

	// small sleep so timestamps differ measurably
	time.Sleep(2 * time.Millisecond)

	_ = task.Transition(tasks.StateProcessing)

	if !task.LastUpdateTime().After(before) {
		t.Error("expected LastUpdateTime to advance after transition")
	}
}

func TestTransition_DoesNotMutateCreationTime(t *testing.T) {
	task, _ := tasks.New(1, tasks.TaskType(0), tasks.TaskValue(10))
	created := task.CreationTime()

	time.Sleep(2 * time.Millisecond)
	_ = task.Transition(tasks.StateProcessing)

	if task.CreationTime() != created {
		t.Error("CreationTime should never change after construction")
	}
}

// --- String ---

func TestString_ContainsKeyFields(t *testing.T) {
	task, _ := tasks.New(7, tasks.TaskType(3), tasks.TaskValue(42))
	s := task.String()

	checks := []string{"7", "3", "42", "received"}
	for _, want := range checks {
		found := false
		for i := 0; i <= len(s)-len(want); i++ {
			if s[i:i+len(want)] == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("String() = %q, expected to contain %q", s, want)
		}
	}
}
