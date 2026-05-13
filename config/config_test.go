package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ekefan/marbl/config"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	return path
}

func TestLoadProducer_ValidFile(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")
	t.Setenv("RABBITMQ_DSN", "amqp://env-rabbitmq")

	path := writeFile(t, "producer.yaml", `
rabbitmq:
  queue_name: "tasks"

producer:
  rate_per_second: 5
  max_backlog: 50
`)

	cfg, err := config.LoadProducer(path)
	if err != nil {
		t.Fatalf("LoadProducer() error: %v", err)
	}

	if cfg.Database.DSN != "postgres://env-postgres" {
		t.Errorf("database dsn: got %q", cfg.Database.DSN)
	}

	if cfg.RabbitMQ.DSN != "amqp://env-rabbitmq" {
		t.Errorf("rabbitmq dsn: got %q", cfg.RabbitMQ.DSN)
	}

	if cfg.Producer.RatePerSecond != 5 {
		t.Errorf("rate_per_second: got %d, want 5", cfg.Producer.RatePerSecond)
	}

	if cfg.Producer.MaxBacklog != 50 {
		t.Errorf("max_backlog: got %d, want 50", cfg.Producer.MaxBacklog)
	}
}

func TestLoadProducer_DefaultsApplied(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")
	t.Setenv("RABBITMQ_DSN", "amqp://env-rabbitmq")

	path := writeFile(t, "producer.yaml", `
rabbitmq:
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

func TestLoadProducer_MissingPostgresDSN(t *testing.T) {
	t.Setenv("RABBITMQ_DSN", "amqp://env-rabbitmq")

	path := writeFile(t, "producer.yaml", `
rabbitmq:
  queue_name: "tasks"

producer:
  rate_per_second: 1
  max_backlog: 10
`)

	_, err := config.LoadProducer(path)

	if err == nil {
		t.Error("expected error for missing POSTGRES_DSN, got nil")
	}
}

func TestLoadProducer_MissingRabbitMQDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")

	path := writeFile(t, "producer.yaml", `
rabbitmq:
  queue_name: "tasks"

producer:
  rate_per_second: 1
  max_backlog: 10
`)

	_, err := config.LoadProducer(path)

	if err == nil {
		t.Error("expected error for missing RABBITMQ_DSN, got nil")
	}
}

func TestLoadProducer_InvalidMaxBacklog(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")
	t.Setenv("RABBITMQ_DSN", "amqp://env-rabbitmq")

	path := writeFile(t, "producer.yaml", `
rabbitmq:
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

func TestLoadProducer_MetricsAddr(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")
	t.Setenv("RABBITMQ_DSN", "amqp://env-rabbitmq")

	path := writeFile(t, "producer.yaml", `
rabbitmq:
  queue_name: "tasks"

producer:
  rate_per_second: 1
  max_backlog: 10

metrics:
  port: 9099
`)

	cfg, err := config.LoadProducer(path)
	if err != nil {
		t.Fatalf("LoadProducer() error: %v", err)
	}

	if cfg.Metrics.Addr() != ":9099" {
		t.Errorf("Addr(): got %q, want :9099", cfg.Metrics.Addr())
	}
}

func TestLoadConsumer_ValidFile(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")
	t.Setenv("RABBITMQ_DSN", "amqp://env-rabbitmq")

	path := writeFile(t, "consumer.yaml", `
rabbitmq:
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
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")
	t.Setenv("RABBITMQ_DSN", "amqp://env-rabbitmq")

	path := writeFile(t, "consumer.yaml", `
rabbitmq:
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
	t.Setenv("POSTGRES_DSN", "postgres://env-postgres")

	path := writeFile(t, "consumer.yaml", `
rabbitmq:
  queue_name: "tasks"

consumer:
  rate_limit: 5
`)

	_, err := config.LoadConsumer(path)

	if err == nil {
		t.Error("expected error for missing RABBITMQ_DSN, got nil")
	}
}

func TestLoadConsumer_EmptyPathUsesDefaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://from-env")
	t.Setenv("RABBITMQ_DSN", "amqp://from-env")

	cfg, err := config.LoadConsumer("")
	if err != nil {
		t.Fatalf("LoadConsumer() with empty path error: %v", err)
	}

	if cfg.Database.DSN != "postgres://from-env" {
		t.Errorf("DSN from env: got %q", cfg.Database.DSN)
	}

	if cfg.RabbitMQ.DSN != "amqp://from-env" {
		t.Errorf("rabbitmq dsn from env: got %q", cfg.RabbitMQ.DSN)
	}
}