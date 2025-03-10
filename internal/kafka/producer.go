package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"

	"mb/internal/config"
)

func ProduceMessages(ctx context.Context, cfg *config.Config) {
	producer, err := createProducer(cfg)
	if err != nil {
		log.Fatalf("Ошибка создания отправителя: %v", err)
	}
	defer producer.Close()

	for i := 0; i < MessageCount; i++ {
		if err := sendMessage(ctx, producer, cfg, i); err != nil {
			log.Printf("Ошибка отправки сообщения: %v", err)
		}
	}
}

func createProducer(cfg *config.Config) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer([]string{cfg.Kafka.Broker}, GetSaramaConfig())
}

func sendMessage(ctx context.Context, producer sarama.SyncProducer, cfg *config.Config, index int) error {
	msg := &sarama.ProducerMessage{
		Topic: cfg.Kafka.TopicName,
		Value: sarama.StringEncoder(fmt.Sprintf("Сообщение %d", index)),
	}

	err := retry(DefaultRetryCount, DefaultRetryDelay, func() error {
		_, _, err := producer.SendMessage(msg)
		return err
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return err
	}
}

func retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		if attempts--; attempts > 0 {
			time.Sleep(sleep)
			return retry(attempts, 2*sleep, f)
		}
		return err
	}
	return nil
}
