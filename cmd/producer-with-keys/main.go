package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"mb/internal/app"
	"mb/internal/config"
)

func main() {
	// Парсим аргументы командной строки
	numMessages := flag.Int("n", 10, "Количество сообщений для отправки")
	numKeys := flag.Int("keys", 3, "Количество разных ключей")
	flag.Parse()

	// Настраиваем логгер
	log := app.SetupLogger()

	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	// Создаем контекст с обработкой сигналов
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("Запуск производителя с ключами...")

	// Создаем конфигурацию Sarama
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Retry.Max = 3
	saramaConfig.Version = sarama.V2_1_0_0

	// Создаем производителя
	producer, err := sarama.NewSyncProducer([]string{cfg.Kafka.Broker}, saramaConfig)
	if err != nil {
		log.Fatalf("Ошибка создания производителя: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Errorf("Ошибка закрытия производителя: %v", err)
		}
	}()

	log.Infof("Отправляем %d сообщений с %d разными ключами в тему %s", *numMessages, *numKeys, cfg.Kafka.TopicName)

	// Отправляем сообщения с разными ключами
	for i := 0; i < *numMessages; i++ {
		// Проверяем завершение контекста
		select {
		case <-ctx.Done():
			log.Info("Получен сигнал завершения, останавливаем отправку")
			return
		default:
			// Выбираем ключ из доступных (циклически)
			keyIndex := i % *numKeys
			key := fmt.Sprintf("key-%d", keyIndex)

			// Создаем сообщение
			value := fmt.Sprintf("Сообщение %d с ключом %s, время: %s", i, key, time.Now().Format(time.RFC3339))

			// Создаем сообщение Kafka с ключом
			msg := &sarama.ProducerMessage{
				Topic: cfg.Kafka.TopicName,
				Key:   sarama.StringEncoder(key),
				Value: sarama.StringEncoder(value),
			}

			// Отправляем сообщение
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Errorf("Ошибка отправки сообщения: %v", err)
				continue
			}

			log.Infof("Сообщение отправлено: ключ=%s, раздел=%d, смещение=%d, значение=%s",
				key, partition, offset, value)

			// Добавляем небольшую задержку между сообщениями
			time.Sleep(500 * time.Millisecond)
		}
	}

	log.Info("Все сообщения отправлены успешно")
}
