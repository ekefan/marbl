package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"

	"github.com/ekefan/marbl/contracts"
	"github.com/ekefan/marbl/storage/generated"
	"github.com/ekefan/marbl/tasks"
)

// PostgresRepository implements contracts.TaskRepository backed by postgres.
type PostgresRepository struct {
	db      *pgxpool.Pool
	queries *generated.Queries
}

var _ contracts.TaskRepository = (*PostgresRepository)(nil)

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


func (r *PostgresRepository) RunMigrations(dsn string, migrationsPath string) error {
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

func (r *PostgresRepository) DB() *pgxpool.Pool{
	return r.db
}

func (r *PostgresRepository) Create(ctx context.Context, taskType tasks.TaskType, value tasks.TaskValue) (*tasks.Task, error) {
	row, err := r.queries.CreateTask(ctx, generated.CreateTaskParams{
		Type:  int16(taskType),
		Value: int16(value),
	})
	if err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}

	return reconstitute(row)
}

func (r *PostgresRepository) UpdateState(ctx context.Context, id int64, next tasks.TaskState) error {
	_, err := r.queries.UpdateTaskState(ctx, generated.UpdateTaskStateParams{
		ID:    id,
		State: generated.TaskState(next),
	})
	if err != nil {
		return fmt.Errorf("update task %d state to %q: %w", id, next, err)
	}

	return nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, id int64) (*tasks.Task, error) {
	row, err := r.queries.GetTaskByID(ctx, id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("task %d not found", id)
		}
		return nil, fmt.Errorf("get task %d: %w", id, err)
	}

	return reconstitute(row)
}

func (r *PostgresRepository) CountByState(ctx context.Context) (map[tasks.TaskState]int64, error) {
	rows, err := r.queries.CountByState(ctx)
	if err != nil {
		return nil, fmt.Errorf("count by state: %w", err)
	}

	counts := map[tasks.TaskState]int64{
		tasks.StateReceived:   0,
		tasks.StateProcessing: 0,
		tasks.StateDone:       0,
	}

	for _, row := range rows {
		counts[tasks.TaskState(row.State)] = row.Count
	}

	return counts, nil
}

func reconstitute(row generated.Task) (*tasks.Task, error) {
	t, err := tasks.Reconstitute(
		row.ID,
		tasks.TaskType(row.Type),
		tasks.TaskValue(row.Value),
		tasks.TaskState(row.State),
		row.CreationTime.Time.UTC(),
		row.LastUpdateTime.Time.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("reconstitute task %d from storage: %w", row.ID, err)
	}

	return t, nil
}
