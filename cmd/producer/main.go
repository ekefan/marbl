package main

import (
	"context"
	"errors"
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
	"github.com/ekefan/marbl/pkg/mlogger"
	"github.com/ekefan/marbl/storage"
	"github.com/ekefan/marbl/transport"
)

// version is injected at build time:
// go build -ldflags="-s -w -X main.version=$(git describe --tags --always)" ./cmd/producer
var version = "dev"

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	versionFlag := flag.Bool("version", false, "print build version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Println(version)
		os.Exit(0)
	}

	if err := run(*cfgPath); err != nil {
		if errors.Is(err, orchestration.ErrMaxBacklogReached) {
			slog.Info("producer finished: max backlog reached")
			os.Exit(0)
		}
		slog.Error("producer exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(cfgPath string) error {
	cfg, err := config.LoadProducer(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := mlogger.BuildLogger(cfg.Logging)
	slog.SetDefault(logger)
	logger.Info("starting producer", slog.String("version", version))

	
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	prodMetrics, err := metrics.NewProducerMetrics(reg)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}

	metricsSrv := metrics.NewServer(cfg.Metrics.Addr(), reg, logger)
	metricsSrv.Start()
	logger.Info("prometheus metric server listening", slog.String("addr", cfg.Metrics.Addr()))

	go func() {
		logger.Info("pprof listening", slog.String("addr", cfg.Profiling.Addr()))
		if err := http.ListenAndServe(cfg.Profiling.Addr(), nil); err != nil {
			logger.Error("pprof server error", slog.String("error", err.Error()))
		}
	}()

	
	repo, err := storage.NewPostgresRepository(cfg.Database.DSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer repo.Close()

	pub, err := transport.NewPublisher(transport.PublisherConfig{
		DSN:       cfg.RabbitMQ.DSN,
		QueueName: cfg.RabbitMQ.QueueName,
		Logger:    logger,
	})
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", err)
	}
	defer pub.Close()

	producer := orchestration.NewProducer(
		repo,
		pub,
		orchestration.ProducerConfig{
			MaxBacklog: cfg.Producer.MaxBacklog,
			Rate:       cfg.Producer.RatePerSecond,
			Logger:     logger,
			OnProduce: func() {
				prodMetrics.TasksProduced.Inc()
				prodMetrics.TasksReceived.Inc()
			},
		},
	)

	// --- graceful shutdown ---
	// --- Handle running workers
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runErr := producer.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)

	return runErr
}