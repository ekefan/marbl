package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/ekefan/marbl/infrastructure/postgres/generated"
	"github.com/ekefan/marbl/domain"
)

var (
	ErrTasksNoUpdate = errors.New("no tasks fit for this update")
)

// PostgresRepository implements contracts.TaskRepository backed by postgres.
type PostgresRepository struct {
	db      *pgxpool.Pool
	queries *generated.Queries
}

var _ domain.TaskRepository = (*PostgresRepository)(nil)

func NewPostgresRepository(dsn string) (*PostgresRepository, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	config.MaxConns = 20
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	fmt.Println("Connected to postgres")

	return &PostgresRepository{
		db:      pool,
		queries: generated.New(pool),
	}, nil
}

func (r *PostgresRepository) runMigrations(dsn string, migrationsPath string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("migration driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(migrationsPath, "postgres", driver)
	if err != nil {
		return fmt.Errorf("migration init: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migration up: %w", err)
	}

	return nil
}

func (r *PostgresRepository) Close() error {
	r.db.Close()
	return nil
}

func (r *PostgresRepository) Create(ctx context.Context, taskType domain.TaskType, value domain.TaskValue) (*domain.Task, error) {
	row, err := r.queries.CreateTask(ctx, generated.CreateTaskParams{
		Type:  int16(taskType),
		Value: int16(value),
	})
	if err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}

	return reconstitute(row)
}

func (r *PostgresRepository) UpdateState(ctx context.Context, id int64, next domain.TaskState) error {
	_, err := r.queries.UpdateTaskState(ctx, generated.UpdateTaskStateParams{
		ID:    id,
		Column2: generated.TaskState(next),
	})
	if err != nil {
		// here, I could separate tasks updates between states to their own functions to ensure this check for processing 
		// but with more time, I would do it...
		if errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("task_no_update",
				slog.String("task_id", fmt.Sprintf("%d", id)),
				slog.String("next_task_state", fmt.Sprintf("%v", next)),
				)
			return ErrTasksNoUpdate
		}
		return fmt.Errorf("update task %d state to %q: %w", id, next, err)
	}

	return nil
}

func (r *PostgresRepository) SumValueByType(ctx context.Context) (map[domain.TaskType]int64, error) {
	rows, err := r.queries.SumValueByType(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"sum value by type: %w",
			err,
		)
	}
	result := make(map[domain.TaskType]int64)

	for _, row := range rows {
		result[domain.TaskType(row.Type)] = row.Total
	}
	return result, nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, id int64) (*domain.Task, error) {
	row, err := r.queries.GetTaskByID(ctx, id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("task %d not found", id)
		}
		return nil, fmt.Errorf("get task %d: %w", id, err)
	}

	return reconstitute(row)
}

func (r *PostgresRepository) CountByState(ctx context.Context) (map[domain.TaskState]int64, error) {
	rows, err := r.queries.CountByState(ctx)
	if err != nil {
		return nil, fmt.Errorf("count by state: %w", err)
	}

	counts := map[domain.TaskState]int64{
		domain.StateReceived:   0,
		domain.StateProcessing: 0,
		domain.StateDone:       0,
	}

	for _, row := range rows {
		counts[domain.TaskState(row.State)] = row.Count
	}

	return counts, nil
}

func reconstitute(row generated.Task) (*domain.Task, error) {
	t, err := domain.Reconstitute(
		row.ID,
		domain.TaskType(row.Type),
		domain.TaskValue(row.Value),
		domain.TaskState(row.State),
		row.CreationTime.Time.UTC(),
		row.LastUpdateTime.Time.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("reconstitute task %d from storage: %w", row.ID, err)
	}

	return t, nil
}
