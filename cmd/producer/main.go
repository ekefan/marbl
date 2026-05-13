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

	_ "github.com/joho/godotenv/autoload"

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
		if errors.Is(err, application.ErrMaxBacklogReached) {
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

	pub, err := rabbitmq.NewPublisher(rabbitmq.PublisherConfig{
		DSN:       cfg.RabbitMQ.DSN,
		QueueName: cfg.RabbitMQ.QueueName,
		Logger:    logger,
	})
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", err)
	}
	defer pub.Close()

	producer := application.NewProducer(
		repo,
		pub,
		application.ProducerConfig{
			MaxBacklog: cfg.Producer.MaxBacklog,
			Rate:       cfg.Producer.RatePerSecond,
			Logger:     logger,
			OnProduce: func() {
				prodMetrics.TasksProduced.Inc()
			},
		},
	)

	// graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runErr := producer.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)

	return runErr
}