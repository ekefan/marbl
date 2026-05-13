.PHONY: \
	all \
	build \
	build-producer \
	build-consumer \
	version-check \
	mock-publisher-contract \
	sqlc-generate \
	test \
	test-short \
	test-cover \
	lint \
	migrate-status \
	schema-init-tasks \
	schema-add-comments \
	docker-up \
	docker-down \
	docker-consumer-logs \
	docker-producer-logs \
	docker-clean \
	flamegraph-producer \
	flamegraph-consumer \
	profile-producer-mem \
	profile-consumer-mem \
	trace-producer \
	clean \
	help

MODULE      := github.com/ekefan/marbl
VERSION     := v1.0.0
LDFLAGS     := -ldflags="-s -w -X main.version=$(VERSION)"

PRODUCER_BIN := bin/producer
CONSUMER_BIN := bin/consumer

MIGRATE_DSN ?= postgres://marbl:marbl@localhost:5432/marbl?sslmode=disable
MIGRATIONS  := infrastructure/postgres/migrations

all: lint test build

build: build-producer build-consumer

build-producer:
	@mkdir -p bin
	go build $(LDFLAGS) -o $(PRODUCER_BIN) ./cmd/producer

build-consumer:
	@mkdir -p bin
	go build $(LDFLAGS) -o $(CONSUMER_BIN) ./cmd/consumer

version-check: build
	@echo "producer: $$(./$(PRODUCER_BIN) -version)"
	@echo "consumer: $$(./$(CONSUMER_BIN) -version)"

mock-publisher-contract:
	mockgen \
		-source=contracts/task_publisher.go \
		-destination=internal/mocks/task_publisher.go \
		-package=mocks

mock-contracts:
	mockgen \
		-destination=internal/mocks/contracts.go \
		-package=mocks \
		github.com/ekefan/marbl/contracts \
		TaskPublisher,QueueDepthChecker,TaskSubscriber

sqlc-generate:
	sqlc generate

test:
	go test -v -race -count=1 -timeout=120s ./...

test-short:
	go test -v -race -count=1 -short ./...

test-cover:
	go test -race -count=1 -timeout=120s -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	go vet ./...
	staticcheck ./... 2>/dev/null || true

migrate-status:
	migrate -path $(MIGRATIONS) -database "$(MIGRATE_DSN)" version

schema-init-tasks:
	migrate create -ext sql -dir $(MIGRATIONS) -seq init_task

schema-add-comments:
	migrate create -ext sql -dir $(MIGRATIONS) -seq add_comment


go-run-consumer:
	go run cmd/consumer/main.go --config cmd/consumer/config.yaml

go-run-producer:
	go run cmd/consumer/main.go --config cmd/consumer/config.yaml

docker-consumer-logs:
	docker compose logs -f consumer

docker-producer-logs:
	docker compose logs -f producer

docker-clean:
	docker compose down -v --remove-orphans

compose-local:
	docker compose -p marbl-local -f compose.local.yml up -d
compose-local-down:
	docker compose -p marbl-local -f compose.local.yml down

compose-dev-down:
	docker compose -p marbl-dev -f compose.dev.yml down

compose-dev-infra-up:
	docker compose -p marbl-dev -f compose.dev.yml up  -d postgres rabbitmq prometheus grafana
	@echo "Waiting for Postgres..."
	@until docker exec $$(docker ps -q -f name=marbl-dev-postgres-1) \
		pg_isready -U marbl; do \
		echo "waiting..."; sleep 2; \
	done
	@echo "Running migrations..."
	DB_URL=$(POSTGRES_DSN) ./migrate.sh up

compose-dev-app-up-build:
	docker compose -p marbl-dev -f compose.dev.yml up -d producer consumer --build

compose-dev-app-up:
	docker compose -p marbl-dev -f compose.dev.ymlup -f compose.dev.yml -d producer consumer

flamegraph-producer:
	curl -s "http://localhost:6061/debug/pprof/profile?seconds=30" -o producer.prof
	go tool pprof -http=:8080 producer.prof

flamegraph-consumer:
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

clean:
	rm -rf bin/ coverage.out coverage.html *.prof *.heap *.trace

help:
	@echo "make build"
	@echo "make test"
	@echo "make test-short"
	@echo "make test-cover"
	@echo "make lint"
	@echo "make sqlc-generate"
	@echo "make migrate-status"
	@echo "make schema-init-tasks"
	@echo "make schema-add-comments"
	@echo "make mock-publisher-contract"
	@echo "make mock-contracts"
	@echo "make docker-up"
	@echo "make docker-down"
	@echo "make docker-clean"
	@echo "make flamegraph-producer"
	@echo "make flamegraph-consumer"
	@echo "make profile-producer-mem"
	@echo "make profile-consumer-mem"
	@echo "make trace-producer"
	@echo "make clean"