package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ekefan/marbl/config"
)

// writeFile writes content to a temp file and returns its path.
func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func TestLoadProducer_ValidFile(t *testing.T) {
	path := writeFile(t, "producer.yaml", `
database:
  dsn: "postgres://user:pass@localhost:5432/marbl"
rabbitmq:
  dsn: "amqp://guest:guest@localhost:5672/"
  queue_name: "tasks"
producer:
  rate_per_second: 5
  max_backlog: 50
`)
	cfg, err := config.LoadProducer(path)
	if err != nil {
		t.Fatalf("LoadProducer() error: %v", err)
	}

	if cfg.Producer.RatePerSecond != 5 {
		t.Errorf("rate_per_second: got %d, want 5", cfg.Producer.RatePerSecond)
	}
	if cfg.Producer.MaxBacklog != 50 {
		t.Errorf("max_backlog: got %d, want 50", cfg.Producer.MaxBacklog)
	}
}

func TestLoadProducer_DefaultsApplied(t *testing.T) {
	path := writeFile(t, "producer.yaml", `
database:
  dsn: "postgres://user:pass@localhost:5432/marbl"
rabbitmq:
  dsn: "amqp://guest:guest@localhost:5672/"
  queue_name: "tasks"
producer:
  rate_per_second: 2
  max_backlog: 10
`)
	cfg, err := config.LoadProducer(path)
	if err != nil {
		t.Fatalf("LoadProducer() error: %v", err)
	}

	if cfg.Metrics.Port != 9091 {
		t.Errorf("default metrics port: got %d, want 9091", cfg.Metrics.Port)
	}
	if cfg.Profiling.Port != 6061 {
		t.Errorf("default profiling port: got %d, want 6061", cfg.Profiling.Port)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("default log format: got %q, want json", cfg.Logging.Format)
	}
}

func TestLoadProducer_MissingDSN(t *testing.T) {
	path := writeFile(t, "producer.yaml", `
rabbitmq:
  dsn: "amqp://guest:guest@localhost:5672/"
  queue_name: "tasks"
producer:
  rate_per_second: 1
  max_backlog: 10
`)
	_, err := config.LoadProducer(path)
	if err == nil {
		t.Error("expected error for missing database.dsn, got nil")
	}
}

func TestLoadProducer_InvalidMaxBacklog(t *testing.T) {
	path := writeFile(t, "producer.yaml", `
database:
  dsn: "postgres://user:pass@localhost:5432/marbl"
rabbitmq:
  dsn: "amqp://guest:guest@localhost:5672/"
  queue_name: "tasks"
producer:
  rate_per_second: 1
  max_backlog: 0
`)
	_, err := config.LoadProducer(path)
	if err == nil {
		t.Error("expected error for max_backlog=0, got nil")
	}
}

func TestLoadProducer_EnvOverridesFile(t *testing.T) {
	path := writeFile(t, "producer.yaml", `
database:
  dsn: "postgres://file-value"
rabbitmq:
  dsn: "amqp://guest:guest@localhost:5672/"
  queue_name: "tasks"
producer:
  rate_per_second: 1
  max_backlog: 10
`)
	t.Setenv("PRODUCER_DATABASE_DSN", "postgres://env-value")

	cfg, err := config.LoadProducer(path)
	if err != nil {
		t.Fatalf("LoadProducer() error: %v", err)
	}

	if cfg.Database.DSN != "postgres://env-value" {
		t.Errorf("env override: got %q, want postgres://env-value", cfg.Database.DSN)
	}
}

func TestLoadProducer_MetricsAddr(t *testing.T) {
	path := writeFile(t, "producer.yaml", `
database:
  dsn: "postgres://x"
rabbitmq:
  dsn: "amqp://x"
  queue_name: "tasks"
producer:
  rate_per_second: 1
  max_backlog: 10
metrics:
  port: 9099
`)
	cfg, _ := config.LoadProducer(path)
	if cfg.Metrics.Addr() != ":9099" {
		t.Errorf("Addr(): got %q, want :9099", cfg.Metrics.Addr())
	}
}

func TestLoadConsumer_ValidFile(t *testing.T) {
	path := writeFile(t, "consumer.yaml", `
database:
  dsn: "postgres://user:pass@localhost:5432/marbl"
rabbitmq:
  dsn: "amqp://guest:guest@localhost:5672/"
  queue_name: "tasks"
consumer:
  rate_limit: 20
  rate_burst: 20
  prefetch: 15
`)
	cfg, err := config.LoadConsumer(path)
	if err != nil {
		t.Fatalf("LoadConsumer() error: %v", err)
	}

	if cfg.Consumer.RateLimit != 20 {
		t.Errorf("rate_limit: got %d, want 20", cfg.Consumer.RateLimit)
	}
	if cfg.Consumer.Prefetch != 15 {
		t.Errorf("prefetch: got %d, want 15", cfg.Consumer.Prefetch)
	}
}

func TestLoadConsumer_DefaultsApplied(t *testing.T) {
	path := writeFile(t, "consumer.yaml", `
database:
  dsn: "postgres://user:pass@localhost:5432/marbl"
rabbitmq:
  dsn: "amqp://guest:guest@localhost:5672/"
  queue_name: "tasks"
consumer:
  rate_limit: 5
`)
	cfg, err := config.LoadConsumer(path)
	if err != nil {
		t.Fatalf("LoadConsumer() error: %v", err)
	}

	if cfg.Metrics.Port != 9092 {
		t.Errorf("default metrics port: got %d, want 9092", cfg.Metrics.Port)
	}
	if cfg.Consumer.RateBurst != 10 {
		t.Errorf("default rate_burst: got %d, want 10", cfg.Consumer.RateBurst)
	}
}

func TestLoadConsumer_MissingRabbitMQDSN(t *testing.T) {
	path := writeFile(t, "consumer.yaml", `
database:
  dsn: "postgres://user:pass@localhost:5432/marbl"
rabbitmq:
  queue_name: "tasks"
consumer:
  rate_limit: 5
`)
	_, err := config.LoadConsumer(path)
	if err == nil {
		t.Error("expected error for missing rabbitmq.dsn, got nil")
	}
}

func TestLoadConsumer_EmptyPathUsesDefaults(t *testing.T) {
	t.Setenv("CONSUMER_DATABASE_DSN", "postgres://from-env")
	t.Setenv("CONSUMER_RABBITMQ_DSN", "amqp://from-env")
	t.Setenv("CONSUMER_RABBITMQ_QUEUE_NAME", "tasks")
	t.Setenv("CONSUMER_CONSUMER_RATE_LIMIT", "5")

	cfg, err := config.LoadConsumer("")
	if err != nil {
		t.Fatalf("LoadConsumer() with empty path error: %v", err)
	}

	if cfg.Database.DSN != "postgres://from-env" {
		t.Errorf("DSN from env: got %q", cfg.Database.DSN)
	}
}