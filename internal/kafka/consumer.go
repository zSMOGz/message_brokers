package kafka

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/IBM/sarama"

	"mb/internal/config"
)

func ConsumeMessages(ctx context.Context, cfg *config.Config) {
	log.Println("Подключение к Kafka...")
	consumer, err := sarama.NewConsumer([]string{cfg.Kafka.Broker}, GetSaramaConfig())
	if err != nil {
		log.Fatalf("Ошибка создания потребителя: %v", err)
	}
	defer consumer.Close()
	log.Println("Успешно подключен к Kafka")

	log.Println("Создание потребителя для раздела...")
	partitionConsumer, err := consumer.ConsumePartition(
		cfg.Kafka.TopicName,
		DefaultPartition,
		sarama.OffsetOldest,
	)
	if err != nil {
		log.Fatalf("Ошибка создания потребителя для раздела: %v", err)
	}
	defer partitionConsumer.Close()
	log.Println("Успешно создан потребитель для раздела")

	// Используем количество воркеров из конфигурации
	messages := make(chan *sarama.ConsumerMessage, cfg.Kafka.NumWorkers*2)

	log.Printf("Запуск %d воркеров...", cfg.Kafka.NumWorkers)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Kafka.NumWorkers; i++ {
		wg.Add(1)
		go worker(ctx, &wg, messages, i)
	}
	log.Println("Воркеры успешно запущены")

	// Чтение сообщений и отправка в канал
	for {
		select {
		case <-ctx.Done():
			log.Println("Получен сигнал завершения работы...")
			close(messages)
			wg.Wait()
			return
		case msg := <-partitionConsumer.Messages():
			log.Printf("Получено сообщение из Kafka: %s", string(msg.Value))
			messages <- msg
		case err := <-partitionConsumer.Errors():
			log.Printf("Ошибка получения сообщения: %v", err)
		}
	}
}

func worker(ctx context.Context, wg *sync.WaitGroup, messages <-chan *sarama.ConsumerMessage, id int) {
	defer wg.Done()
	log.Printf("Воркер %d запущен", id)
	for msg := range messages {
		select {
		case <-ctx.Done():
			log.Printf("Воркер %d завершает работу", id)
			return
		default:
			log.Printf("Воркер %d начал обработку сообщения: %s", id, string(msg.Value))
			err := processMessage(msg, id)
			if err != nil {
				log.Printf("Ошибка обработки сообщения: %v", err)
			}
			log.Printf("Воркер %d завершил обработку сообщения: %s", id, string(msg.Value))
		}
	}
	log.Printf("Воркер %d завершил работу", id)
}

func processMessage(msg *sarama.ConsumerMessage, workerID int) error {
	log.Printf("Воркер %d: Получено сообщение: %s", workerID, string(msg.Value))

	// Пример обработки: преобразование сообщения в верхний регистр
	processedMessage := string(msg.Value)
	processedMessage = strings.ToUpper(processedMessage)

	log.Printf("Воркер %d: Обработанное сообщение: %s", workerID, processedMessage)

	// Возвращаем nil, если обработка прошла успешно
	return nil
}
