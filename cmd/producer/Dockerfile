# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

# install build dependencies
RUN apk add --no-cache git

# cache dependencies before copying source
COPY go.mod go.sum ./
RUN go mod download

# copy full source
COPY . .

# build with version injection and binary size optimisations
ARG VERSION=dev
RUN go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /bin/producer \
    ./cmd/producer

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM alpine:3.19

# non-root user for security
RUN addgroup -S marbl && adduser -S marbl -G marbl

WORKDIR /app

# copy binary and migrations (needed for RunMigrations)
COPY --from=builder /bin/producer         ./producer
COPY --from=builder /app/storage/migrations ./storage/migrations
COPY --from=builder /app/cmd/producer/config.yaml ./cmd/producer/config.yaml

USER marbl

EXPOSE 9091 6061

ENTRYPOINT ["./producer"]
CMD ["--config", "cmd/producer/config.yaml"]