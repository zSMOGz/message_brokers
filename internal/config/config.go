package config

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	defaultConfigName = "config"
	defaultConfigPath = "."
)

type KafkaConfig struct {
	Broker        string `mapstructure:"broker"`
	TopicName     string `mapstructure:"topic_name"`
	MaxAttempts   int    `mapstructure:"max_attempts"`
	NumWorkers    int    `mapstructure:"num_workers"`
	ConsumerGroup string `mapstructure:"consumer_group"`
}

type Config struct {
	Kafka KafkaConfig `mapstructure:"kafka"`
}

func Load() (*Config, error) {
	viper.SetConfigName(defaultConfigName)
	viper.AddConfigPath(defaultConfigPath)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("ошибка чтения конфигурации: %w", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("ошибка разбора конфигурации: %w", err)
	}
	return &cfg, nil
}
