# Marbl — Task Pipeline

A producer/consumer task pipeline built in Go, demonstrating service design,
observability, profiling, and database migrations.

## Quick Start

```bash
# 1. copy and configure env
cp .env.example .env

# 2. start the full stack
make up

# 3. open dashboards
# Grafana:    http://localhost:3000  (admin / admin)
# RabbitMQ:   http://localhost:15672 (guest / guest)
# Prometheus: http://localhost:9090
```

To run services locally without Docker:

```bash
make up-infra               # start postgres + rabbitmq only
make build                  # build both binaries to bin/
./bin/producer --config cmd/producer/config.yaml
./bin/consumer --config cmd/consumer/config.yaml
```

---

## Design Decisions

### Communication — RabbitMQ

RabbitMQ was chosen over gRPC/HTTP for three reasons that align with the
project constraints:

1. **Backpressure is native** — the broker queue depth is the backlog. The
   producer checks `QueueDepth()` before each publish and stops cleanly when
   it hits `max_backlog`. No application-level counter needed.
2. **Decoupled lifecycles** — the producer can finish and exit while the
   consumer keeps draining. Neither service needs to know if the other is
   running.
3. **Durability** — messages are persisted to disk (`DeliveryMode: Persistent`)
   and survive broker restarts. Combined with manual acks, no task is silently
   lost if the consumer crashes mid-processing.

The shared database is an **observability store**, not a coordination
mechanism. Both services write state independently — the producer on creation,
the consumer on receipt and completion.

### Database — PostgreSQL with sqlc + golang-migrate

- `sqlc` generates type-safe Go from SQL queries — no ORM, no reflection at
  runtime, compile-time query validation.
- `golang-migrate` handles schema versioning. Migrations live in
  `storage/migrations/` and are run by the producer on startup.
- `pgxpool` is used over `database/sql` for native PostgreSQL support,
  better connection pool controls, and no driver abstraction overhead.
- The consumer never runs migrations — the producer owns the schema lifecycle
  to avoid concurrent migration races on startup.

### Logging — `log/slog` (stdlib)

The task spec explicitly asks to use the standard library as much as possible.
`slog` (added in Go 1.21) provides structured JSON logging natively with no
external dependency. Log format (json/console) and level are runtime-configurable.

### Architecture — Hexagonal with descriptive package names

```
tasks/          core entity, state machine, validation — zero external deps
contracts/      interfaces: TaskRepository, TaskPublisher, QueueDepthChecker
storage/        postgres adapter (implements contracts.TaskRepository)
transport/      rabbitmq adapter (implements contracts.TaskPublisher/Subscriber)
orchestration/  producer and consumer service logic
config/         viper-based config loader
metrics/        prometheus registrations
cmd/            dependency wiring, main()
```

The domain (`tasks/`) never imports infrastructure. Metric hooks are injected
as `OnProduce` / `OnDone` callbacks from `cmd/` — the orchestration layer has
zero prometheus imports.

### Concurrency

- **Producer**: single goroutine ticking at the configured rate. No shared
  state — one goroutine, one channel (the ticker).
- **Consumer**: RabbitMQ prefetch controls how many messages are in-flight.
  The token bucket rate limiter (`golang.org/x/time/rate`) controls processing
  speed at the application layer. Per-type aggregation uses `sync.RWMutex` —
  fast writes, concurrent reads for metrics.
- **Channels vs mutexes**: channels are used where goroutines need to
  communicate (ticker → produce loop). Mutexes are used where goroutines share
  memory (stats map). Both are used where appropriate rather than forcing one
  pattern everywhere.

### Build Flags

Binaries are built with:

```bash
go build -ldflags="-s -w -X main.version=$(VERSION)" ./cmd/producer
```

- `-s` strips the symbol table
- `-w` strips DWARF debug info
- Together they reduce binary size by ~30%
- `-X main.version` injects the git tag at link time

---

## Configuration

Both services are configured via YAML with environment variable overrides.

| Layer | Priority | Example |
|-------|----------|---------|
| Environment variable | Highest | `PRODUCER_DATABASE_DSN=postgres://...` |
| Config file | Middle | `cmd/producer/config.yaml` |
| Default | Lowest | `rate_per_second: 1` |

Environment variables follow the pattern `<SERVICE>_<SECTION>_<KEY>`:

```bash
PRODUCER_PRODUCER_RATE_PER_SECOND=5
CONSUMER_CONSUMER_RATE_LIMIT=10
```

---

## Database Migrations

```bash
# apply all pending migrations
make migrate-up

# roll back one migration (live demo: add/remove comment column)
make migrate-down

# check current version
make migrate-status
```

The demo migration adds a `comment TEXT` column:

```bash
make migrate-up    # runs 000002_add_comment.up.sql
make migrate-down  # runs 000002_add_comment.down.sql
```

Both can be run while services are operating — `ALTER TABLE ADD COLUMN` and
`DROP COLUMN` are non-blocking in PostgreSQL 16.

---

## Profiling

Both services expose pprof on a separate port (producer: 6061, consumer: 6062).

### Flame Graph

```bash
# capture 30s CPU profile and open flame graph in browser
make flamegraph-producer
make flamegraph-consumer
```

Or manually:

```bash
# capture trace
curl -o trace.out http://localhost:6061/debug/pprof/trace?seconds=10
go tool trace trace.out

# capture CPU profile and generate flame graph
go tool pprof http://localhost:6061/debug/pprof/profile?seconds=30
# inside pprof shell:
(pprof) web        # opens SVG flame graph
```

### Memory Profile

```bash
make profile-heap-producer
```

### GOGC and GOMEMLIMIT

`GOGC` controls the GC trigger threshold — the heap size ratio at which a
collection is triggered. Default is 100 (collect when heap doubles).

```bash
# more frequent GC — lower memory usage, higher CPU cost
GOGC=50 ./bin/producer

# less frequent GC — higher throughput, more memory used
GOGC=200 ./bin/producer

# disable GC entirely (use with GOMEMLIMIT)
GOGC=off GOMEMLIMIT=200MiB ./bin/producer
```

`GOMEMLIMIT` (Go 1.19+) sets a soft memory ceiling. The GC will run more
aggressively before this limit is hit, preventing OOM kills.

```bash
# cap memory at 200MB — GC becomes more aggressive near the limit
GOMEMLIMIT=200MiB ./bin/producer
```

**Recommended approach for this workload**: keep `GOGC` at default (100) and
set `GOMEMLIMIT` to ~75% of available container memory. This prevents OOM
without paying the CPU cost of an artificially low GC threshold.

```yaml
# docker-compose environment:
environment:
  GOMEMLIMIT: 200MiB
  GOGC: "100"
```

---

## Testing

```bash
make test              # all tests with race detector
make test-unit         # unit tests only (no docker required)
make test-integration  # integration tests (requires docker)
make test-cover        # generate HTML coverage report
```

Integration tests use testcontainers — a real postgres and rabbitmq instance
spins up for the test run and is torn down after. One container per package,
shared across all tests in that package via `TestMain`.

---

## Version

```bash
./bin/producer -version
./bin/consumer -version
```

---

## Project Structure

```
.
├── cmd/
│   ├── producer/          # producer binary entrypoint + config.yaml
│   └── consumer/          # consumer binary entrypoint + config.yaml
├── tasks/                 # core domain entity
├── contracts/             # port interfaces
├── storage/               # postgres adapter, migrations, sqlc
├── transport/             # rabbitmq publisher + subscriber
├── orchestration/         # producer + consumer service logic
├── config/                # viper config loader
├── metrics/               # prometheus setup
├── prometheus/            # prometheus scrape config
├── grafana/               # grafana provisioning + dashboards
├── Dockerfile.producer
├── Dockerfile.consumer
├── docker-compose.yml
├── Makefile
└── .env.example
```