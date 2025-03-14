package main

import (
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"

	"mb/internal/app"
)

func main() {
	log, cfg, ctx, stop := app.InitApp()
	defer stop()
	log.Info("Консольный потребитель запущен, ожидание сообщений...")

	// Создаем конфигурацию Sarama
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Version = sarama.V2_1_0_0
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Используем другую группу для консольного потребителя
	consumerGroup := "console-consumer-group"

	// Создаем группу потребителей
	log.Infof("Создаем группу потребителей: %s", consumerGroup)
	client, err := sarama.NewConsumerGroup([]string{cfg.Kafka.Broker}, consumerGroup, saramaConfig)
	if err != nil {
		log.Fatalf("Ошибка создания группы потребителей: %v", err)
	}
	defer client.Close()

	// Создаем обработчик
	handler := &ConsoleHandler{log: log}

	// Запускаем потребление в отдельной горутине
	go func() {
		for {
			log.Infof("Начинаем потребление из топика %s", cfg.Kafka.TopicName)
			err := client.Consume(ctx, []string{cfg.Kafka.TopicName}, handler)
			if err != nil {
				log.Errorf("Ошибка при потреблении: %v", err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Ожидаем сигнал завершения
	<-ctx.Done()
	log.Info("Консольный потребитель завершает работу...")
}

type ConsoleHandler struct {
	log *logrus.Logger
}

func (h *ConsoleHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.log.Infof("Настройка сессии для разделов: %v", session.Claims())
	return nil
}

func (h *ConsoleHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.log.Info("Очистка сессии")
	return nil
}

func (h *ConsoleHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.log.Infof("Начало обработки раздела %d", claim.Partition())

	for message := range claim.Messages() {
		h.log.Printf("\n== СООБЩЕНИЕ ==\n")
		h.log.Printf("Тема: %s\n", message.Topic)
		h.log.Printf("Раздел: %d\n", message.Partition)
		h.log.Printf("Смещение: %d\n", message.Offset)
		h.log.Printf("Содержимое: %s\n", string(message.Value))
		h.log.Printf("==============\n\n")

		session.MarkMessage(message, "")
	}

	return nil
}
