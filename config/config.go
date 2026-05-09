package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// ProducerConfig is the full configuration for the producer service.
type ProducerConfig struct {
	Database   DatabaseConfig   `mapstructure:"database"`
	RabbitMQ   RabbitMQConfig   `mapstructure:"rabbitmq"`
	Metrics    MetricsConfig    `mapstructure:"metrics"`
	Profiling  ProfilingConfig  `mapstructure:"profiling"`
	Logging    LoggingConfig    `mapstructure:"logging"`
	Producer   ProducerSettings `mapstructure:"producer"`
}

// ConsumerConfig is the full configuration for the consumer service.
type ConsumerConfig struct {
	Database  DatabaseConfig   `mapstructure:"database"`
	RabbitMQ  RabbitMQConfig   `mapstructure:"rabbitmq"`
	Metrics   MetricsConfig    `mapstructure:"metrics"`
	Profiling ProfilingConfig  `mapstructure:"profiling"`
	Logging   LoggingConfig    `mapstructure:"logging"`
	Consumer  ConsumerSettings `mapstructure:"consumer"`
}

// DatabaseConfig holds postgres connection settings.
type DatabaseConfig struct {
	DSN             string `mapstructure:"dsn"`
	MigrationsPath  string `mapstructure:"migrations_path"`
}

// RabbitMQConfig holds broker connection and queue settings.
type RabbitMQConfig struct {
	DSN       string `mapstructure:"dsn"`
	QueueName string `mapstructure:"queue_name"`
}

// MetricsConfig holds prometheus exposure settings.
// Endpoint is fixed to /metrics per the spec.
type MetricsConfig struct {
	Port int `mapstructure:"port"`
}

func (m MetricsConfig) Addr() string {
	return fmt.Sprintf(":%d", m.Port)
}

// ProfilingConfig holds pprof server settings.
type ProfilingConfig struct {
	Port int `mapstructure:"port"`
}

func (p ProfilingConfig) Addr() string {
	return fmt.Sprintf(":%d", p.Port)
}

// LoggingConfig controls log level and format.
type LoggingConfig struct {
	// Level: debug, info, warn, error
	Level string `mapstructure:"level"`
	// Format: json or console
	Format string `mapstructure:"format"`
}

// ProducerSettings holds producer-specific runtime settings.
type ProducerSettings struct {
	// RatePerSecond is how many tasks to generate per second.
	RatePerSecond int `mapstructure:"rate_per_second"`
	// MaxBacklog is the maximum unprocessed messages in the broker queue
	// before the producer stops.
	MaxBacklog int64 `mapstructure:"max_backlog"`
}

// ConsumerSettings holds consumer-specific runtime settings.
type ConsumerSettings struct {
	// RateLimit is the maximum number of tasks processed per second.
	RateLimit int `mapstructure:"rate_limit"`
	// RateBurst is the token bucket burst size. Defaults to RateLimit.
	RateBurst int `mapstructure:"rate_burst"`
	// Prefetch is the RabbitMQ QoS prefetch count.
	Prefetch int `mapstructure:"prefetch"`
}

// LoadProducer loads producer config from the given file path.
// Environment variables prefixed with PRODUCER_ override file values.
// Example: PRODUCER_DATABASE_DSN overrides database.dsn
func LoadProducer(cfgPath string) (*ProducerConfig, error) {
	v := newViper("PRODUCER")
	setProducerDefaults(v)

	if err := loadFile(v, cfgPath); err != nil {
		return nil, err
	}

	var cfg ProducerConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal producer config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid producer config: %w", err)
	}

	return &cfg, nil
}

// LoadConsumer loads consumer config from the given file path.
// Environment variables prefixed with CONSUMER_ override file values.
func LoadConsumer(cfgPath string) (*ConsumerConfig, error) {
	v := newViper("CONSUMER")
	setConsumerDefaults(v)

	if err := loadFile(v, cfgPath); err != nil {
		return nil, err
	}

	var cfg ConsumerConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal consumer config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer config: %w", err)
	}

	return &cfg, nil
}

// --- validation ---

func (c *ProducerConfig) validate() error {
	if c.Database.DSN == "" {
		return fmt.Errorf("database.dsn is required")
	}
	if c.RabbitMQ.DSN == "" {
		return fmt.Errorf("rabbitmq.dsn is required")
	}
	if c.RabbitMQ.QueueName == "" {
		return fmt.Errorf("rabbitmq.queue_name is required")
	}
	if c.Producer.MaxBacklog <= 0 {
		return fmt.Errorf("producer.max_backlog must be > 0")
	}
	if c.Producer.RatePerSecond <= 0 {
		return fmt.Errorf("producer.rate_per_second must be > 0")
	}
	return nil
}

func (c *ConsumerConfig) validate() error {
	if c.Database.DSN == "" {
		return fmt.Errorf("database.dsn is required")
	}
	if c.RabbitMQ.DSN == "" {
		return fmt.Errorf("rabbitmq.dsn is required")
	}
	if c.RabbitMQ.QueueName == "" {
		return fmt.Errorf("rabbitmq.queue_name is required")
	}
	if c.Consumer.RateLimit <= 0 {
		return fmt.Errorf("consumer.rate_limit must be > 0")
	}
	return nil
}

func newViper(envPrefix string) *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	return v
}

func loadFile(v *viper.Viper, cfgPath string) error {
	if cfgPath == "" {
		return nil
	}
	v.SetConfigFile(cfgPath)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config file %q: %w", cfgPath, err)
	}
	return nil
}

func setProducerDefaults(v *viper.Viper) {
	// required fields — empty string defaults so AutomaticEnv binds them
	// even when no config file is provided
	v.SetDefault("database.dsn", "")
	v.SetDefault("rabbitmq.dsn", "")
	v.SetDefault("rabbitmq.queue_name", "tasks")

	v.SetDefault("metrics.port", 9091)
	v.SetDefault("profiling.port", 6061)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("producer.rate_per_second", 1)
	v.SetDefault("producer.max_backlog", 100)
	v.SetDefault("database.migrations_path", "file://storage/migrations")
}

func setConsumerDefaults(v *viper.Viper) {
	// required fields — empty string defaults so AutomaticEnv binds them
	// even when no config file is provided
	v.SetDefault("database.dsn", "")
	v.SetDefault("rabbitmq.dsn", "")
	v.SetDefault("rabbitmq.queue_name", "tasks")

	v.SetDefault("metrics.port", 9092)
	v.SetDefault("profiling.port", 6062)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("consumer.rate_limit", 10)
	v.SetDefault("consumer.rate_burst", 10)
	v.SetDefault("consumer.prefetch", 10)
	v.SetDefault("database.migrations_path", "file://storage/migrations")
}