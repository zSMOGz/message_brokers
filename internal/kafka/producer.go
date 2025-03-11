package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"mb/internal/config"
)

type Producer struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
}

func NewProducer(cfg *config.Config) (*Producer, error) {
	syncProducer, err := createSyncProducer(cfg)
	if err != nil {
		return nil, err
	}

	asyncProducer, err := createAsyncProducer(cfg)
	if err != nil {
		syncProducer.Close()
		return nil, err
	}

	return &Producer{
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
	}, nil
}

func (p *Producer) Close() {
	if p.syncProducer != nil {
		p.syncProducer.Close()
	}
	if p.asyncProducer != nil {
		p.asyncProducer.Close()
	}
}

func (p *Producer) SendSync(ctx context.Context, cfg *config.Config, index int, prefix string) error {
	msg := &sarama.ProducerMessage{
		Topic: cfg.Kafka.TopicName,
		Value: sarama.StringEncoder(fmt.Sprintf("%s Сообщение %d", prefix, index)),
	}

	err := retry(DefaultRetryCount, DefaultRetryDelay, func() error {
		_, _, err := p.syncProducer.SendMessage(msg)
		return err
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return err
	}
}

func (p *Producer) SendAsync(ctx context.Context, cfg *config.Config, index int, prefix string) {
	msg := &sarama.ProducerMessage{
		Topic: cfg.Kafka.TopicName,
		Value: sarama.StringEncoder(fmt.Sprintf("%s Сообщение %d", prefix, index)),
	}

	select {
	case <-ctx.Done():
		return
	case p.asyncProducer.Input() <- msg:
	}
}

func createSyncProducer(cfg *config.Config) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer([]string{cfg.Kafka.Broker}, GetSaramaConfig())
}

func createAsyncProducer(cfg *config.Config) (sarama.AsyncProducer, error) {
	return sarama.NewAsyncProducer([]string{cfg.Kafka.Broker}, GetSaramaConfig())
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
