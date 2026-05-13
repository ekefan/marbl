FROM golang:1.26.3-alpine AS builder


WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /bin/consumer \
    ./cmd/consumer

FROM alpine:3.19

RUN addgroup -S marbl && adduser -S marbl -G marbl

WORKDIR /app

COPY --from=builder /bin/consumer              ./consumer
# COPY --from=builder /app/cmd/consumer/config.yaml ./cmd/consumer/config.yaml

USER marbl

EXPOSE 9092 6062

ENTRYPOINT ["./consumer"]
CMD ["--config", "/app/config.yaml"]