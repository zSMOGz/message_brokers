package app

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"mb/internal/config"
	"mb/internal/kafka"
)

type App struct {
	log *logrus.Logger
	cfg *config.Config
}

func New(log *logrus.Logger, cfg *config.Config) *App {
	return &App{log: log, cfg: cfg}
}

func (a *App) InitializeKafka(ctx context.Context) error {
	if err := kafka.WaitForBroker(ctx, a.cfg.Kafka.Broker, a.cfg.Kafka.MaxAttempts); err != nil {
		return err
	}
	return nil
}

func (a *App) RunProducerConsumer(ctx context.Context) {
	producer, err := kafka.NewProducer(a.cfg)
	if err != nil {
		a.log.Fatalf("Ошибка создания производителя: %v", err)
	}
	defer producer.Close()

	// Каждый отправитель отправляет половину сообщений
	go a.runSyncProducer(ctx, producer, kafka.MessageCount/2)
	go a.runAsyncProducer(ctx, producer, kafka.MessageCount/2)

	time.Sleep(2 * time.Second)
	kafka.ConsumeMessages(ctx, a.cfg)
}

func (a *App) runSyncProducer(ctx context.Context, producer *kafka.Producer, count int) {
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			if err := producer.SendSync(ctx, a.cfg, i, "[SYNC]"); err != nil {
				a.log.Printf("Ошибка синхронной отправки сообщения: %v", err)
				continue
			}
		}
	}
}

func (a *App) runAsyncProducer(ctx context.Context, producer *kafka.Producer, count int) {
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			producer.SendAsync(ctx, a.cfg, i, "[ASYNC]")
		}
	}
}
