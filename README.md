# Task Procssor Pipeline

## Prerequisites

These tools are needed before running anything.

### Required

| Tool | Version | Install |
|------|---------|---------|
| **Go** | 1.22+ | https://go.dev/dl/ |
| **Docker** | 24+ | https://docs.docker.com/get-docker/ |
| **Docker Compose** | v2 (bundled with Docker Desktop) | — |
| **Make** | any | `brew install make` / `apt install make` |

### Go toolchain (install after Go is set up)

```bash
# SQL query code generator
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

# DB migration CLI
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# Mock generator (for tests)
go install github.com/golang/mock/mockgen@latest
```

Add `$(go env GOPATH)/bin` to your `PATH` if it isn't already:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

---

## Infrastructure Overview

```
┌─────────────┐     ┌──────────────┐     ┌────────────────┐
│  Producer   │────▶│   RabbitMQ   │────▶│   Consumer     │
│  :8080      │     │  :5672/15672 │     │  :8081         │
└──────┬──────┘     └──────────────┘     └───────┬────────┘
       │                                          │
       │          ┌──────────────┐                │
       └─────────▶│  PostgreSQL  │◀───────────────┘
                  │  :5432       │
                  └──────────────┘
                        │
              ┌─────────▼──────────┐
              │    Prometheus      │
              │    :9090           │
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │     Grafana        │
              │    :3000           │
              └────────────────────┘
```
## Communication Protocol Choice – RabbitMQ

I chose RabbitMQ as the communication protocol between the producer and consumer because the system is designed as an asynchronous task pipeline where producers should not depend on consumer availability or processing speed. RabbitMQ naturally supports decoupling through message queues, allowing the producer to continue generating tasks while the consumer processes them at its own rate. It also provides built-in buffering, acknowledgements, and requeueing, which simplifies handling failures and ensures tasks are not lost while still enabling controlled backlog limits and rate-based consumption. Compared to HTTP or gRPC, which are more request-response and tightly coupled, RabbitMQ better fits the need for independent scaling and resilience. I did not consider ZeroMQ for this implementation as it was outside the scope of my familiarity and would have required additional learning time and risk for this take-home exercise.
---

## Environment Variables

Copy `.env.example` to `.env` and adjust as needed:

```bash
## for local setup
cp .env.example .env
```
```bash
## for dev(containers only) setup
cp .env.example .env.dev
```

---

## Quickstart

You could start the application in two possible ways: <br>
1. Running the producer and consumer binaries on your machine and infra(postgres, rabbitmq, prometheus and grafana) running from a docker container
2. Running the producer and everything from docker containers

Either ways you could edit the yaml config files for each of them.
Producer: cmd/producer/config.yaml
Consumer: cmd/consumer/config.yaml
Allow please set .env to run in "1" and .env.dev to run in "2"

"1" Running producer and consumer binaries on you machine
```bash
# 1. Read compose.local.yml to bring up all infrastructure (Postgres, RabbitMQ, Prometheus, Grafana) and run binaries with make go-run-consumer make go-run-producer
make compose-local
./migrate.sh up ## run migrations on the database.

make go-run-consumer ## run consumer
make go-run-producer ## run producer

## OR BUILD and run
make build
#then
./bin/producer --config cmd/producer/config.yaml
./bin/consumer --config cmd/producer/config.yaml

## To drop the compose stack for local
make compose-local-down
```

"2" Running the binaries and infrastructure from docker containers
```bash
# spins up infra, and runs migrations against the db
make compose-dev-infra-up

make compose-dev-app-up-build ## run once to build the binaries
## OR use

make compose-dev-app ## to run them if running for the second time


## To drop the compose stack for the dev setup
make compose-dev-down
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| RabbitMQ Management UI | http://localhost:15672 | `guest` / `guest` |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| PostgreSQL | `localhost:5432` | `marbl` / `marbl` / db `marbl` |

---

## Testing

```bash
# Run all tests
make test
```

---

## Grafana Dashboards

Four panels are pre-configured in `monitoring/grafana/dashboards/tasks.json`:

1. **Message States** — count of tasks in `received`, `processing`, `done`
2. **Service Health** — up/down status of producer and consumer
3. **Value Sum per Task Type** — total `value` accumulated per type (0–9)
4. **Processed Tasks per Task Type** — task count per type

Import the dashboard JSON manually, or it auto-loads via the provisioning config in `monitoring/grafana/provisioning/`.