package domain_test

import (
	"testing"
	"time"

	"github.com/ekefan/marbl/domain"
)

func TestNew_ValidInputs(t *testing.T) {
	task, err := domain.NewTask(1, domain.TaskType(3), domain.TaskValue(50))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if task.ID() != 1 {
		t.Errorf("expected id=1, got %d", task.ID())
	}
	if task.Type() != domain.TaskType(3) {
		t.Errorf("expected type=3, got %d", task.Type())
	}
	if task.Value() != domain.TaskValue(50) {
		t.Errorf("expected value=50, got %d", task.Value())
	}
	if task.State() != domain.StateReceived {
		t.Errorf("expected state=received, got %q", task.State())
	}
}

func TestNew_BoundaryTaskTypes(t *testing.T) {
	cases := []struct {
		name     string
		taskType domain.TaskType
		wantErr  bool
	}{
		{"min valid", domain.TaskType(0), false},
		{"max valid", domain.TaskType(9), false},
		{"below min", domain.TaskType(-1), true},
		{"above max", domain.TaskType(10), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := domain.NewTask(1, tc.taskType, domain.TaskValue(50))
			if (err != nil) != tc.wantErr {
				t.Errorf("NewTask() error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestNew_BoundaryTaskValues(t *testing.T) {
	cases := []struct {
		name      string
		taskValue domain.TaskValue
		wantErr   bool
	}{
		{"min valid", domain.TaskValue(0), false},
		{"max valid", domain.TaskValue(99), false},
		{"below min", domain.TaskValue(-1), true},
		{"above max", domain.TaskValue(100), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := domain.NewTask(1, domain.TaskType(5), tc.taskValue)
			if (err != nil) != tc.wantErr {
				t.Errorf("NewTask() error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestNew_TimestampsSetOnCreation(t *testing.T) {
	before := time.Now().UTC()
	task, err := domain.NewTask(1, domain.TaskType(0), domain.TaskValue(0))
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

func TestTransition_HappyPath(t *testing.T) {
	task, _ := domain.NewTask(1, domain.TaskType(0), domain.TaskValue(10))

	if err := task.Transition(domain.StateProcessing); err != nil {
		t.Fatalf("received -> processing failed: %v", err)
	}
	if task.State() != domain.StateProcessing {
		t.Errorf("expected processing, got %q", task.State())
	}

	if err := task.Transition(domain.StateDone); err != nil {
		t.Fatalf("processing -> done failed: %v", err)
	}
	if task.State() != domain.StateDone {
		t.Errorf("expected done, got %q", task.State())
	}
}

func TestTransition_SkipStateNotAllowed(t *testing.T) {
	task, _ := domain.NewTask(1, domain.TaskType(0), domain.TaskValue(10))

	// try jumping directly received -> done
	err := task.Transition(domain.StateDone)
	if err == nil {
		t.Error("expected error skipping processing state, got nil")
	}
}

func TestTransition_BackwardsNotAllowed(t *testing.T) {
	task, _ := domain.NewTask(1, domain.TaskType(0), domain.TaskValue(10))
	_ = task.Transition(domain.StateProcessing)

	// try going back to received
	err := task.Transition(domain.StateReceived)
	if err == nil {
		t.Error("expected error transitioning backwards, got nil")
	}
}

func TestTransition_TerminalStateBlocked(t *testing.T) {
	task, _ := domain.NewTask(1, domain.TaskType(0), domain.TaskValue(10))
	_ = task.Transition(domain.StateProcessing)
	_ = task.Transition(domain.StateDone)

	// done is terminal — no further moves
	err := task.Transition(domain.StateDone)
	if err == nil {
		t.Error("expected error transitioning from terminal state, got nil")
	}
}

func TestTransition_UpdatesLastUpdateTime(t *testing.T) {
	task, _ := domain.NewTask(1, domain.TaskType(0), domain.TaskValue(10))
	before := task.LastUpdateTime()

	// small sleep so timestamps differ measurably
	time.Sleep(2 * time.Millisecond)

	_ = task.Transition(domain.StateProcessing)

	if !task.LastUpdateTime().After(before) {
		t.Error("expected LastUpdateTime to advance after transition")
	}
}

func TestTransition_DoesNotMutateCreationTime(t *testing.T) {
	task, _ := domain.NewTask(1, domain.TaskType(0), domain.TaskValue(10))
	created := task.CreationTime()

	time.Sleep(2 * time.Millisecond)
	_ = task.Transition(domain.StateProcessing)

	if task.CreationTime() != created {
		t.Error("CreationTime should never change after construction")
	}
}

// --- String ---

func TestString_ContainsKeyFields(t *testing.T) {
	task, _ := domain.NewTask(7, domain.TaskType(3), domain.TaskValue(42))
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
