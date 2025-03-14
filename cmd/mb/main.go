package main

import (
	"mb/internal/app"
	"mb/internal/metrics"
)

func main() {
	log, cfg, ctx, stop := app.InitApp()
	defer stop()
	log.Println("Приложение запущено, ожидание сигнала завершения...")

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
