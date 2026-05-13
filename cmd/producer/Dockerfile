FROM golang:1.26.3-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /bin/producer \
    ./cmd/producer

FROM alpine:3.19

RUN addgroup -S marbl && adduser -S marbl -G marbl

WORKDIR /app

COPY --from=builder /bin/producer         ./producer
# COPY --from=builder /app/storage/migrations ./storage/migrations
# COPY --from=builder /app/cmd/producer/config.yaml ./cmd/producer/config.yaml

USER marbl

EXPOSE 9091 6061

ENTRYPOINT ["./producer"]
CMD ["--config", "/app/config.yaml"]