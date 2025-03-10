package kafka

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/IBM/sarama"
)

const (
	defaultDialTimeout = 5 * time.Second
	defaultReadTimeout = 5 * time.Second
	kafkaVersion       = "2.8.0"
)

func WaitForBroker(ctx context.Context, broker string, maxAttempts int) error {
	config := createSaramaConfig()

	for i := 0; i < maxAttempts; i++ {
		if err := tryConnect(ctx, broker, config, i); err == nil {
			return nil
		}
	}
	return fmt.Errorf("не удалось подключиться к Kafka после %d попыток", maxAttempts)
}

func createSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	version, _ := sarama.ParseKafkaVersion(kafkaVersion)
	config.Version = version
	config.Net.DialTimeout = defaultDialTimeout
	config.Net.ReadTimeout = defaultReadTimeout
	return config
}

func tryConnect(ctx context.Context, broker string, config *sarama.Config, attempt int) error {
	client, err := sarama.NewClient([]string{broker}, config)
	if err == nil {
		client.Close()
		return nil
	}

	waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(waitTime):
		return err
	}
}

func GetSaramaConfig() *sarama.Config {
	config := createSaramaConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	return config
}
