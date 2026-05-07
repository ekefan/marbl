/<br>
├── cmd/<br>
│   ├── producer/<br>
│   │   └── main.go<br>
│   └── consumer/<br>
│       └── main.go<br>
├── tasks/                 </t> # core entity, states, types, business rules<br>
│   ├── task.go<br>
│   └── task_test.go<br>
├── contracts/              # interfaces — what storage/transport must implement<br>
│   ├── repository.go<br>
│   └── publisher.go<br>
├── storage/                # postgres adapter, sqlc generated code, migrations<br>
│   ├── migrations/<br>
│   ├── queries/<br>
│   ├── generated/<br>
│   ├── repository.go       # implements contracts.TaskRepository<br>
│   └── repository_test.go<br>
├── transport/              # gRPC client + server, proto definitions<br>
│   ├── proto/<br>
│   ├── server.go<br>
│   └── client.go<br>
├── orchestration/          # producer and consumer service logic<br>
│   ├── producer.go<br>
│   ├── producer_test.go<br>
│   ├── consumer.go<br>
│   └── consumer_test.go<br>
├── config/                 # viper config structs, loader<br>
│   └── config.go<br>
├── metrics/                # prometheus setup, shared metric helpers<br>
│   └── metrics.go<br>
├── docker-compose.yml<br>
├── prometheus/<br>
├── grafana/<br>
└── Makefile<br>


## Architecture Decisions Take at start:

1. Communication Protocol - gRPC (HTTP/2)
Reason:
    - strongly typed contracts via protobuf
    - No documenation overhead required
    - Fits latancy-sensitive environment 
    - Generate stubs, less surface for bugs
    - And tight but flexible coupling as but services share same database

2. Database - PostgresSQL (in Docker)
Reason:
    - Generally, postgres > sqlite for concurrency support
    - Works well with sqlc + golang migrate
    - Connection pooling, proper enum types and concurrent writes

3. Logging - log/slog
Reaons:
    - slog give structured JSON logs natively
    - no external dep needed(zerolog/zap would be a nice alternative)
    - Configurable handlers for console vs JSON at runtime



---How would backlog be handled with gRPC?