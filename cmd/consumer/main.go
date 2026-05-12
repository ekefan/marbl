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

	"github.com/ekefan/marbl/application"
	"github.com/ekefan/marbl/config"
	"github.com/ekefan/marbl/infrastructure/postgres"
	"github.com/ekefan/marbl/infrastructure/rabbitmq"
	"github.com/ekefan/marbl/internal/mlogger"
	"github.com/ekefan/marbl/metrics"
)

var version string

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
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

	logger := mlogger.BuildLogger(cfg.Logging)
	slog.SetDefault(logger)
	logger.Info("starting consumer", slog.String("version", version))

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	consumerMetrics, err := metrics.NewConsumerMetrics(reg)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}

	metricsSrv := metrics.NewServer(cfg.Metrics.Addr(), reg, logger)
	metricsSrv.Start()

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

	sub, err := rabbitmq.NewSubscriber(rabbitmq.SubscriberConfig{
		DSN:       cfg.RabbitMQ.DSN,
		QueueName: cfg.RabbitMQ.QueueName,
		Prefetch:  cfg.Consumer.Prefetch,
		Logger:    logger,
	})
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", err)
	}
	defer sub.Close()

	consumer := application.NewConsumer(repo, application.ConsumerConfig{
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := sub.Serve(ctx, consumer.Handler())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)

	return serveErr
}