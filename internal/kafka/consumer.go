package kafka

import (
	"context"
	"log"

	"github.com/IBM/sarama"

	"mb/internal/config"
)

func ConsumeMessages(ctx context.Context, cfg *config.Config) {
	consumer, err := createConsumer(cfg)
	if err != nil {
		log.Fatalf("Ошибка создания потребителя: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := createPartitionConsumer(consumer, cfg)
	if err != nil {
		log.Fatalf("Ошибка создания потребителя для раздела: %v", err)
	}
	defer partitionConsumer.Close()

	processMessages(ctx, partitionConsumer)
}

func createConsumer(cfg *config.Config) (sarama.Consumer, error) {
	return sarama.NewConsumer([]string{cfg.Kafka.Broker}, GetSaramaConfig())
}

func createPartitionConsumer(consumer sarama.Consumer, cfg *config.Config) (sarama.PartitionConsumer, error) {
	return consumer.ConsumePartition(
		cfg.Kafka.TopicName,
		DefaultPartition,
		sarama.OffsetOldest,
	)
}

func processMessages(ctx context.Context, partitionConsumer sarama.PartitionConsumer) {
	for i := 0; i < MessageCount; i++ {
		select {
		case <-ctx.Done():
			return
		case msg := <-partitionConsumer.Messages():
			log.Printf("Получено сообщение: %s", string(msg.Value))
		case err := <-partitionConsumer.Errors():
			log.Printf("Ошибка получения сообщения: %v", err)
		}
	}
}
