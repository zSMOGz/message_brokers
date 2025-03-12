package kafka

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

// CreateTopicWithPartitions создает тему с указанным количеством партиций
func CreateTopicWithPartitions(broker string, topicName string, numPartitions int) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	// Создаем администратора
	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	if err != nil {
		return fmt.Errorf("ошибка создания администратора: %w", err)
	}
	defer admin.Close()

	// Создаем тему с указанным числом партиций
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(numPartitions),
		ReplicationFactor: 1, // Для простоты используем фактор репликации 1
	}

	// Создаем тему или получаем информацию, что она уже существует
	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		// Если тема уже существует, пробуем обновить количество партиций
		if err == sarama.ErrTopicAlreadyExists {
			log.Printf("Тема %s уже существует, попытка обновления количества партиций", topicName)

			// Увеличиваем количество партиций, если необходимо
			err = admin.CreatePartitions(topicName, int32(numPartitions), nil, false)
			if err != nil {
				return fmt.Errorf("ошибка обновления партиций: %w", err)
			}
			log.Printf("Количество партиций в теме %s обновлено до %d", topicName, numPartitions)
			return nil
		}
		return fmt.Errorf("ошибка создания темы: %w", err)
	}

	log.Printf("Тема %s успешно создана с %d партициями", topicName, numPartitions)
	return nil
}

// SendMessageToPartition отправляет сообщение в указанную партицию
func (p *Producer) SendMessageToPartition(topicName string, partition int32, message string) error {
	msg := &sarama.ProducerMessage{
		Topic:     topicName,
		Partition: partition,
		Value:     sarama.StringEncoder(message),
	}

	_, _, err := p.syncProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("ошибка отправки сообщения в партицию %d: %w", partition, err)
	}

	log.Printf("Сообщение успешно отправлено в партицию %d: %s", partition, message)
	return nil
}

// SendMessagesToAllPartitions отправляет уникальное сообщение в каждую партицию
func (p *Producer) SendMessagesToAllPartitions(topicName string, numPartitions int) error {
	for i := 0; i < numPartitions; i++ {
		message := fmt.Sprintf("Сообщение для партиции %d", i)
		if err := p.SendMessageToPartition(topicName, int32(i), message); err != nil {
			return err
		}
	}
	return nil
}
