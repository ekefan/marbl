package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type ProducerConfig struct {
	Database  DatabaseConfig   `mapstructure:"database"`
	RabbitMQ  RabbitMQConfig   `mapstructure:"rabbitmq"`
	Metrics   MetricsConfig    `mapstructure:"metrics"`
	Profiling ProfilingConfig  `mapstructure:"profiling"`
	Logging   LoggingConfig    `mapstructure:"logging"`
	Producer  ProducerSettings `mapstructure:"producer"`
}

type ConsumerConfig struct {
	Database  DatabaseConfig   `mapstructure:"database"`
	RabbitMQ  RabbitMQConfig   `mapstructure:"rabbitmq"`
	Metrics   MetricsConfig    `mapstructure:"metrics"`
	Profiling ProfilingConfig  `mapstructure:"profiling"`
	Logging   LoggingConfig    `mapstructure:"logging"`
	Consumer  ConsumerSettings `mapstructure:"consumer"`
}

type DatabaseConfig struct {
	DSN string
}

type RabbitMQConfig struct {
	DSN       string
	QueueName string `mapstructure:"queue_name"`
}

type MetricsConfig struct {
	Port int `mapstructure:"port"`
}

func (m MetricsConfig) Addr() string {
	return fmt.Sprintf(":%d", m.Port)
}

type ProfilingConfig struct {
	Port int `mapstructure:"port"`
}

func (p ProfilingConfig) Addr() string {
	return fmt.Sprintf(":%d", p.Port)
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type ProducerSettings struct {
	RatePerSecond int   `mapstructure:"rate_per_second"`
	MaxBacklog    int64 `mapstructure:"max_backlog"`
}

type ConsumerSettings struct {
	RateLimit int `mapstructure:"rate_limit"`
	RateBurst int `mapstructure:"rate_burst"`
	Prefetch  int `mapstructure:"prefetch"`
}

func LoadProducer(cfgPath string) (*ProducerConfig, error) {
	v := newViper()
	setProducerDefaults(v)

	if err := loadCfgFile(v, cfgPath); err != nil {
		return nil, err
	}

	var cfg ProducerConfig

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal producer config: %w", err)
	}

	cfg.Database.DSN = os.Getenv("POSTGRES_DSN")
	cfg.RabbitMQ.DSN = os.Getenv("RABBITMQ_DSN")

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid producer config: %w", err)
	}

	return &cfg, nil
}

func LoadConsumer(cfgPath string) (*ConsumerConfig, error) {
	v := newViper()
	setConsumerDefaults(v)

	if err := loadCfgFile(v, cfgPath); err != nil {
		return nil, err
	}

	var cfg ConsumerConfig

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal consumer config: %w", err)
	}

	cfg.Database.DSN = os.Getenv("POSTGRES_DSN")
	cfg.RabbitMQ.DSN = os.Getenv("RABBITMQ_DSN")

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer config: %w", err)
	}

	return &cfg, nil
}

func (c *ProducerConfig) validate() error {
	if c.Database.DSN == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}

	if c.RabbitMQ.DSN == "" {
		return fmt.Errorf("RABBITMQ_DSN is required")
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
		return fmt.Errorf("POSTGRES_DSN is required")
	}

	if c.RabbitMQ.DSN == "" {
		return fmt.Errorf("RABBITMQ_DSN is required")
	}

	if c.RabbitMQ.QueueName == "" {
		return fmt.Errorf("rabbitmq.queue_name is required")
	}

	if c.Consumer.RateLimit <= 0 {
		return fmt.Errorf("consumer.rate_limit must be > 0")
	}

	return nil
}

func newViper() *viper.Viper {
	return viper.New()
}

func loadCfgFile(v *viper.Viper, cfgPath string) error {
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
	v.SetDefault("rabbitmq.queue_name", "tasks")

	v.SetDefault("metrics.port", 9091)
	v.SetDefault("profiling.port", 6061)

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")

	v.SetDefault("producer.rate_per_second", 1)
	v.SetDefault("producer.max_backlog", 100)
}

func setConsumerDefaults(v *viper.Viper) {
	v.SetDefault("rabbitmq.queue_name", "tasks")

	v.SetDefault("metrics.port", 9092)
	v.SetDefault("profiling.port", 6062)

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")

	v.SetDefault("consumer.rate_limit", 10)
	v.SetDefault("consumer.rate_burst", 10)
	v.SetDefault("consumer.prefetch", 10)
}