package main

import (
	"context"
	"mb/internal/app"
	"mb/internal/config"
	"mb/internal/metrics"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	setupLogger()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Ошибка конфигурации: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application := app.New(log, cfg)

	if err := application.InitializeKafka(ctx); err != nil {
		log.Fatalf("Ошибка инициализации Kafka: %v", err)
	}

	metrics.Init()

	health := app.SetupHealthCheck(cfg)
	app.StartHealthServer(health)

	application.RunProducerConsumer(ctx)

	<-ctx.Done()
	log.Info("Получен сигнал завершения работы...")
}

func setupLogger() {
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.DebugLevel)
}
