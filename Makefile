schema_init_tasks:
	migrate create -ext sql -dir storage/migrations -seq init_task
schema_add_comments:
	migrate create -ext sql -dir storage/migrations -seq add_comment
sqlc-gen:
	sqlc generate -f storage/sqlc.yaml
# # Version baked in at build time
# VERSION=$(git describe --tags --always)
# go build -ldflags="-s -w -X main.version=$(VERSION)" ./cmd/producer
# # what does version mean?

# # PGO profile would be added in a second  pass when we have a cpu profile






# UP ALTER TABLE tasks ADD COLUMN comment TEXT;
# DOWN ALTER TABLE tasks DROP COLUMN IF EXISTS comment;
.PHONY: all build test lint generate migrate-up migrate-down clean help

# ── Variables ─────────────────────────────────────────────────────────────────

MODULE      := github.com/ekefan/marbl
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS     := -ldflags="-s -w -X main.version=$(VERSION)"

PRODUCER_BIN := bin/producer
CONSUMER_BIN := bin/consumer

MIGRATE_DSN  ?= postgres://marbl:marbl@localhost:5432/marbl?sslmode=disable
MIGRATIONS   := storage/migrations

# ── Default ───────────────────────────────────────────────────────────────────

all: lint test build

# ── Build ─────────────────────────────────────────────────────────────────────

build: build-producer build-consumer

build-producer:
	@echo "→ building producer (version=$(VERSION))"
	@mkdir -p bin
	go build $(LDFLAGS) -o $(PRODUCER_BIN) ./cmd/producer

build-consumer:
	@echo "→ building consumer (version=$(VERSION))"
	@mkdir -p bin
	go build $(LDFLAGS) -o $(CONSUMER_BIN) ./cmd/consumer

version-check: build
	@echo "producer: $$(./$(PRODUCER_BIN) -version)"
	@echo "consumer: $$(./$(CONSUMER_BIN) -version)"

# ── Generate ──────────────────────────────────────────────────────────────────

generate:
	@echo "→ running sqlc generate"
	cd storage && sqlc generate
	@echo "→ running go generate"
	go generate ./...

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	@echo "→ running all tests"
	go test -v -race -count=1 -timeout=120s ./...

test-short:
	@echo "→ running unit tests only (no containers)"
	go test -v -race -count=1 -short ./...

test-cover:
	@echo "→ running tests with coverage"
	go test -race -count=1 -timeout=120s -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "→ coverage report: coverage.html"

# ── Lint ──────────────────────────────────────────────────────────────────────

lint:
	@echo "Running go vet"
	go vet ./...
	@echo "Running staticcheck"
	staticcheck ./... 2>/dev/null || echo "  (staticcheck not installed — skipping)"

# ── Migration ─────────────────────────────────────────────────────────────────

migrate-up:
	@echo "→ running migrations up"
	migrate -path $(MIGRATIONS) -database "$(MIGRATE_DSN)" up

migrate-down:
	@echo "→ rolling back last migration"
	migrate -path $(MIGRATIONS) -database "$(MIGRATE_DSN)" down 1

migrate-status:
	@echo "→ migration status"
	migrate -path $(MIGRATIONS) -database "$(MIGRATE_DSN)" version

# ── Docker ────────────────────────────────────────────────────────────────────

docker-up:
	@echo "→ starting full stack"
	docker compose up --build -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f producer consumer

docker-clean:
	docker compose down -v --remove-orphans

# ── Profiling ─────────────────────────────────────────────────────────────────

flamegraph-producer:
	@echo "→ generating flamegraph for producer (30s)"
	curl -s "http://localhost:6061/debug/pprof/profile?seconds=30" -o producer.prof
	go tool pprof -http=:8080 producer.prof

flamegraph-consumer:
	@echo "→ generating flamegraph for consumer (30s)"
	curl -s "http://localhost:6062/debug/pprof/profile?seconds=30" -o consumer.prof
	go tool pprof -http=:8080 consumer.prof

profile-producer-mem:
	curl -s "http://localhost:6061/debug/pprof/heap" -o producer.heap
	go tool pprof -http=:8080 producer.heap

profile-consumer-mem:
	curl -s "http://localhost:6062/debug/pprof/heap" -o consumer.heap
	go tool pprof -http=:8080 consumer.heap

trace-producer:
	curl -s "http://localhost:6061/debug/pprof/trace?seconds=10" -o producer.trace
	go tool trace producer.trace

# ── Clean ─────────────────────────────────────────────────────────────────────

clean:
	rm -rf bin/ coverage.out coverage.html *.prof *.heap *.trace

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  make build                build both binaries"
	@echo "  make test                 run all tests (requires docker)"
	@echo "  make test-short           run unit tests only"
	@echo "  make test-cover           tests + coverage.html"
	@echo "  make lint                 go vet + staticcheck"
	@echo "  make generate             sqlc + go generate"
	@echo "  make migrate-up           apply pending migrations"
	@echo "  make migrate-down         roll back last migration"
	@echo "  make migrate-status       show current migration version"
	@echo "  make docker-up            start full stack"
	@echo "  make docker-down          stop full stack"
	@echo "  make docker-clean         stop + remove volumes"
	@echo "  make flamegraph-producer  capture + open flamegraph"
	@echo "  make flamegraph-consumer  capture + open flamegraph"
	@echo "  make version-check        verify -version on both binaries"
	@echo "  make clean                remove build artifacts"
	@echo ""