package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/ekefan/marbl/config"
	"github.com/ekefan/marbl/metrics"
	"github.com/ekefan/marbl/orchestration"
	"github.com/ekefan/marbl/storage"
	"github.com/ekefan/marbl/transport"
)

// go build -ldflags="-s -w -X main.version=$(git describe --tags --always)" ./cmd/consumer
var version = "dev_take_home"

func main() {
	cfgPath := flag.String("config", "cmd/consumer/config.yaml", "path to config file")
	versionFlag := flag.Bool("version", false, "print build version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Println(version)
		os.Exit(0)
	}

	if err := run(*cfgPath); err != nil {
		slog.Error("consumer exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(cfgPath string) error {
	cfg, err := config.LoadConsumer(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := buildLogger(cfg.Logging)
	slog.SetDefault(logger)
	logger.Info("starting consumer", slog.String("version", version))

	// --- metrics ---
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	consumerMetrics, err := metrics.NewConsumerMetrics(reg)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}

	metricsSrv := metrics.NewServer(cfg.Metrics.Addr(), reg, logger)
	metricsSrv.Start()

	// --- pprof ---
	go func() {
		logger.Info("pprof listening", slog.String("addr", cfg.Profiling.Addr()))
		if err := http.ListenAndServe(cfg.Profiling.Addr(), nil); err != nil {
			logger.Error("pprof server error", slog.String("error", err.Error()))
		}
	}()

	// --- storage ---
	repo, err := storage.NewPostgresRepository(cfg.Database.DSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer repo.Close()

	// consumer does not run migrations — producer owns schema lifecycle
	// if consumer starts first, it will fail to connect until postgres is ready

	// --- transport ---
	sub, err := transport.NewSubscriber(transport.SubscriberConfig{
		DSN:       cfg.RabbitMQ.DSN,
		QueueName: cfg.RabbitMQ.QueueName,
		Prefetch:  cfg.Consumer.Prefetch,
		Logger:    logger,
	})
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", err)
	}
	defer sub.Close()

	// --- orchestration ---
	consumer := orchestration.NewConsumer(repo, orchestration.ConsumerConfig{
		RateLimit: cfg.Consumer.RateLimit,
		RateBurst: cfg.Consumer.RateBurst,
		Logger:    logger,

		OnProcessing: func() {
			consumerMetrics.TasksProcessing.Inc()
		},

		OnDone: func(taskType int, value int) {
			consumerMetrics.TasksProcessing.Dec()
			consumerMetrics.TasksDone.Inc()
			consumerMetrics.TasksProcessedByType.
				WithLabelValues(fmt.Sprintf("%d", taskType)).Inc()
			consumerMetrics.ValueSumByType.
				WithLabelValues(fmt.Sprintf("%d", taskType)).Add(float64(value))
		},
	})

	// --- graceful shutdown ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := sub.Serve(ctx, consumer.Handler())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)

	return serveErr
}

func buildLogger(cfg config.LoggingConfig) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	if cfg.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}