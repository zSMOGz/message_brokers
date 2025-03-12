package kafka

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/IBM/sarama"

	"mb/internal/config"
)

// Глобальная переменная для отслеживания состояния
var ConsumerReady = false

func ConsumeMessages(ctx context.Context, cfg *config.Config, ready chan<- bool) {
	log.Println("Подключение к Kafka...")
	consumerGroup, err := sarama.NewConsumerGroup([]string{cfg.Kafka.Broker}, cfg.Kafka.ConsumerGroup, GetSaramaConfig())
	if err != nil {
		log.Fatalf("Ошибка создания потребителя: %v", err)
	}
	defer consumerGroup.Close()
	log.Println("Успешно подключен к Kafka")

	// Используем количество воркеров из конфигурации
	messages := make(chan *sarama.ConsumerMessage, cfg.Kafka.NumWorkers*2)

	log.Printf("Запуск %d воркеров...", cfg.Kafka.NumWorkers)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Kafka.NumWorkers; i++ {
		wg.Add(1)
		go worker(ctx, &wg, messages, i)
	}
	log.Println("Воркеры успешно запущены")

	// Создаем обработчик для ConsumerGroup
	handler := &ConsumerGroupHandler{
		messages: messages,
		ready:    ready,
	}

	log.Println("Потребитель успешно запущен")

	// Запускаем потребление сообщений
	for {
		select {
		case <-ctx.Done():
			log.Println("Получен сигнал завершения работы...")
			close(messages)
			wg.Wait()
			return
		default:
			// Потребляем сообщения через ConsumerGroup
			if err := consumerGroup.Consume(ctx, []string{cfg.Kafka.TopicName}, handler); err != nil {
				log.Printf("Ошибка потребления сообщений: %v", err)
			}
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

// Реализация ConsumerGroupHandler
type ConsumerGroupHandler struct {
	messages  chan<- *sarama.ConsumerMessage
	ready     chan<- bool
	setupDone bool
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Println("Настройка ConsumerGroup")

	if !h.setupDone && h.ready != nil {
		log.Println("Отправка сигнала готовности...")
		select {
		case h.ready <- true:
			log.Println("Сигнал готовности отправлен")
		default:
			log.Println("Не удалось отправить сигнал готовности (канал полон или закрыт)")
		}
		h.setupDone = true
	}

	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Очистка ConsumerGroup")
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Println("Начало выполнения ConsumeClaim")
	log.Printf("Подписка на тему: %s, раздел: %d", claim.Topic(), claim.Partition())

	for msg := range claim.Messages() {
		log.Printf("Получено сообщение в ConsumeClaim: %s", string(msg.Value))
		h.messages <- msg
		session.MarkMessage(msg, "")
		log.Printf("Сообщение обработано и помечено в ConsumeClaim: %s", string(msg.Value))
	}

	log.Println("Завершение выполнения ConsumeClaim")
	return nil
}

// OnMessageFunc - функция обратного вызова для обработки сообщений
type OnMessageFunc func(topic string, partition int32, offset int64, value []byte)

// ConsoleConsumerGroupHandler - обработчик для консольного потребителя
type ConsoleConsumerGroupHandler struct {
	OnMessage OnMessageFunc
}

func (h *ConsoleConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Настройка ConsumerGroup")
	return nil
}

func (h *ConsoleConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Очистка ConsumerGroup")
	return nil
}

func (h *ConsoleConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("Начало обработки раздела %d темы %s", claim.Partition(), claim.Topic())

	for msg := range claim.Messages() {
		// Вызываем обработчик сообщений
		if h.OnMessage != nil {
			h.OnMessage(msg.Topic, msg.Partition, msg.Offset, msg.Value)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// ConsumeMessagesConsole читает сообщения и направляет их в консоль
func ConsumeMessagesConsole(ctx context.Context, cfg *config.Config, handler *ConsoleConsumerGroupHandler) {
	log.Println("Подключение к Kafka...")
	consumerGroup, err := sarama.NewConsumerGroup([]string{cfg.Kafka.Broker}, cfg.Kafka.ConsumerGroup, GetSaramaConfig())
	if err != nil {
		log.Fatalf("Ошибка создания потребителя: %v", err)
	}
	defer consumerGroup.Close()
	log.Println("Успешно подключен к Kafka")

	for {
		select {
		case <-ctx.Done():
			log.Println("Получен сигнал завершения работы...")
			return
		default:
			// Потребляем сообщения через ConsumerGroup
			if err := consumerGroup.Consume(ctx, []string{cfg.Kafka.TopicName}, handler); err != nil {
				log.Printf("Ошибка потребления сообщений: %v", err)
			}
		}
	}
}
