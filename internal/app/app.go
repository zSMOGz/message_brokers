package app

import (
	"context"
	"mb/internal/config"
	"mb/internal/kafka"
	"time"

	"github.com/sirupsen/logrus"
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
	go kafka.ProduceMessages(ctx, a.cfg)
	time.Sleep(2 * time.Second)
	kafka.ConsumeMessages(ctx, a.cfg)
}
