package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ekefan/marbl/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testRepo *PostgresRepository

func migrationsPath() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Join(
		filepath.Dir(filename),
		"migrations")
	return fmt.Sprintf("file://%s", dir)
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("taskrunner_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").
				WithStartupTimeout(90 * time.Second),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = container.Terminate(ctx)
	}()

	dsn, err := container.ConnectionString(
		ctx,
		"sslmode=disable",
	)
	if err != nil {
		panic(err)
	}

	repo, err := NewPostgresRepository(dsn)
	if err != nil {
		panic(err)
	}

	defer repo.Close()

	if err := repo.runMigrations(dsn, migrationsPath()); err != nil {
		panic(err)
	}

	testRepo = repo

	code := m.Run()

	os.Exit(code)
}

func resetDB(t *testing.T, ctx context.Context) {
	t.Helper()

	_, err := testRepo.db.Exec(
		ctx,
		`TRUNCATE TABLE tasks RESTART IDENTITY CASCADE`,
	)

	if err != nil {
		t.Fatalf("reset db: %v", err)
	}
}

func TestCreate_InsertsTaskInReceivedState(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)
	task, err := repo.Create(ctx, tasks.TaskType(3), tasks.TaskValue(50))
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if task.ID() == 0 {
		t.Error("expected non-zero ID from DB")
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

func TestCreate_TimestampsAreSet(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	before := time.Now().UTC().Add(-time.Second)
	task, err := repo.Create(ctx, tasks.TaskType(0), tasks.TaskValue(0))
	after := time.Now().UTC().Add(time.Second)

	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if task.CreationTime().Before(before) || task.CreationTime().After(after) {
		t.Errorf("CreationTime %v outside expected range", task.CreationTime())
	}
}

func TestCreate_IDsAreUniqueAndIncreasing(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	t1, _ := repo.Create(ctx, tasks.TaskType(1), tasks.TaskValue(10))
	t2, _ := repo.Create(ctx, tasks.TaskType(2), tasks.TaskValue(20))

	if t1.ID() >= t2.ID() {
		t.Errorf("expected t2.ID > t1.ID, got %d and %d", t1.ID(), t2.ID())
	}
}

func TestGetByID_ReturnsCreatedTask(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	created, _ := repo.Create(ctx, tasks.TaskType(5), tasks.TaskValue(77))

	fetched, err := repo.GetByID(ctx, created.ID())
	if err != nil {
		t.Fatalf("GetByID() error: %v", err)
	}

	if fetched.ID() != created.ID() {
		t.Errorf("ID mismatch: got %d, want %d", fetched.ID(), created.ID())
	}
	if fetched.Type() != created.Type() {
		t.Errorf("Type mismatch: got %d, want %d", fetched.Type(), created.Type())
	}
	if fetched.Value() != created.Value() {
		t.Errorf("Value mismatch: got %d, want %d", fetched.Value(), created.Value())
	}
}

func TestGetByID_MissingTaskReturnsError(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	_, err := repo.GetByID(ctx, 999999)
	if err == nil {
		t.Error("expected error for missing task, got nil")
	}
}

func TestUpdateState_ReceivedToProcessing(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	task, _ := repo.Create(ctx, tasks.TaskType(2), tasks.TaskValue(30))

	if err := repo.UpdateState(ctx, task.ID(), tasks.StateProcessing); err != nil {
		t.Fatalf("UpdateState() error: %v", err)
	}

	updated, _ := repo.GetByID(ctx, task.ID())
	if updated.State() != tasks.StateProcessing {
		t.Errorf("expected processing, got %q", updated.State())
	}
}

func TestUpdateState_ProcessingToDone(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	task, _ := repo.Create(ctx, tasks.TaskType(2), tasks.TaskValue(30))
	_ = repo.UpdateState(ctx, task.ID(), tasks.StateProcessing)
	_ = repo.UpdateState(ctx, task.ID(), tasks.StateDone)

	updated, _ := repo.GetByID(ctx, task.ID())
	if updated.State() != tasks.StateDone {
		t.Errorf("expected done, got %q", updated.State())
	}
}

func TestUpdateState_AdvancesLastUpdateTime(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	task, _ := repo.Create(ctx, tasks.TaskType(1), tasks.TaskValue(10))
	originalUpdate := task.LastUpdateTime()

	// small sleep so DB NOW() differs from creation NOW()
	time.Sleep(10 * time.Millisecond)
	_ = repo.UpdateState(ctx, task.ID(), tasks.StateProcessing)

	updated, _ := repo.GetByID(ctx, task.ID())
	if !updated.LastUpdateTime().After(originalUpdate) {
		t.Error("LastUpdateTime should advance after UpdateState")
	}
}

func TestUpdateState_NoErrorOnCurrentStateProcessing(t *testing.T){
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	task, _ := repo.Create(ctx, tasks.TaskType(2), tasks.TaskValue(3))
	_ = repo.UpdateState(ctx, task.ID(), tasks.StateProcessing)
	err := repo.UpdateState(ctx, task.ID(), tasks.StateProcessing)
	assert.NoError(t, err)
}

func TestUpdateState_DoesNotChangeCreationTime(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	task, _ := repo.Create(ctx, tasks.TaskType(1), tasks.TaskValue(10))
	created := task.CreationTime()

	_ = repo.UpdateState(ctx, task.ID(), tasks.StateProcessing)

	updated, _ := repo.GetByID(ctx, task.ID())
	// TIMESTAMPTZ round-trips exactly — no tolerance needed
	if !updated.CreationTime().Equal(created) {
		t.Errorf("CreationTime changed after UpdateState: %v -> %v", created, updated.CreationTime())
	}
}

func TestCountByState_AllStatesPresent(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	// create 3 received
	t1, _ := repo.Create(ctx, tasks.TaskType(0), tasks.TaskValue(10))
	t2, _ := repo.Create(ctx, tasks.TaskType(1), tasks.TaskValue(20))
	_, _ = repo.Create(ctx, tasks.TaskType(2), tasks.TaskValue(30))

	// move t1 to processing
	_ = repo.UpdateState(ctx, t1.ID(), tasks.StateProcessing)

	// move t2 all the way to done
	_ = repo.UpdateState(ctx, t2.ID(), tasks.StateProcessing)
	_ = repo.UpdateState(ctx, t2.ID(), tasks.StateDone)

	counts, err := repo.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState() error: %v", err)
	}

	if counts[tasks.StateReceived] != 1 {
		t.Errorf("expected 1 received, got %d", counts[tasks.StateReceived])
	}
	if counts[tasks.StateProcessing] != 1 {
		t.Errorf("expected 1 processing, got %d", counts[tasks.StateProcessing])
	}
	if counts[tasks.StateDone] != 1 {
		t.Errorf("expected 1 done, got %d", counts[tasks.StateDone])
	}
}

func TestCountByState_EmptyTableReturnsZeroes(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)

	counts, err := repo.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState() error: %v", err)
	}

	for state, count := range counts {
		if count != 0 {
			t.Errorf("expected 0 for state %q, got %d", state, count)
		}
	}
}

func TestCreate_ConcurrentInsertsDontConflict(t *testing.T) {
	repo := testRepo
	ctx := context.Background()
	resetDB(t, ctx)
	const n = 50
	errs := make(chan error, n)
	ids := make(chan int64, n)

	for i := range n {
		go func(i int) {
			task, err := repo.Create(ctx, tasks.TaskType(i%10), tasks.TaskValue(i%100))
			if err != nil {
				errs <- err
				return
			}
			ids <- task.ID()
			errs <- nil
		}(i)
	}

	seen := make(map[int64]bool)
	for range n {
		if err := <-errs; err != nil {
			t.Errorf("concurrent Create() error: %v", err)
		}
	}
	close(ids)
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID %d from concurrent inserts", id)
		}
		seen[id] = true
	}
}
