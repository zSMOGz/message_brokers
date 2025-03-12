package app

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"mb/internal/config"
	"mb/internal/kafka"
)

// Константы для ожидания готовности потребителя
const (
	MaxConsumerReadyAttempts    = 15
	ConsumerReadyAttemptTimeout = 1 * time.Second
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
	// Убедимся, что Kafka доступна перед запуском
	a.log.Println("Проверяем доступность Kafka")
	if err := a.InitializeKafka(ctx); err != nil {
		a.log.Fatalf("Kafka недоступна: %v", err)
	}
	a.log.Println("Kafka доступна")

	producer, err := kafka.NewProducer(a.cfg)
	if err != nil {
		a.log.Fatalf("Ошибка создания производителя: %v", err)
	}
	defer producer.Close()

	// Канал для сигнализации о готовности
	consumerReady := make(chan bool, 1)

	go kafka.ConsumeMessages(ctx, a.cfg, consumerReady)

	var consumerIsReady bool

	// Ожидание готовности потребителя с несколькими попытками
	for attempt := 0; attempt <= MaxConsumerReadyAttempts; attempt++ {
		a.log.Printf("Ожидаем готовности потребителя... Попытка %d из %d", attempt, MaxConsumerReadyAttempts)

		select {
		case <-consumerReady:
			a.log.Println("Потребитель готов, запускаем продюсеров")
			consumerIsReady = true
		case <-time.After(ConsumerReadyAttemptTimeout):
			a.log.Printf("Таймаут ожидания готовности потребителя (попытка %d), продолжаем ожидание", attempt)
		}

		if consumerIsReady {
			break
		}
	}

	if !consumerIsReady {
		a.log.Printf("Превышено максимальное количество попыток (%d), запускаем продюсеров без подтверждения готовности", MaxConsumerReadyAttempts)
	}

	// Запуск продюсеров
	a.log.Println("Запускаем продюсеров...")
	go a.runSyncProducer(ctx, producer, kafka.MessageCount/2)
	go a.runAsyncProducer(ctx, producer, kafka.MessageCount/2)

	// Ожидание завершения контекста
	<-ctx.Done()
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
