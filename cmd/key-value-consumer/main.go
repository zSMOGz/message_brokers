package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"

	"mb/internal/app"
	"mb/internal/config"
)

func main() {
	// Парсим аргументы командной строки
	topicFlag := flag.String("topic", "", "Имя темы Kafka (если не указано, используется из конфига)")
	groupFlag := flag.String("group", "key-value-consumer-group", "Имя группы потребителей")
	fromBeginningFlag := flag.Bool("from-beginning", true, "Читать сообщения с начала")
	autoCommitFlag := flag.Bool("auto-commit", true, "Автоматически подтверждать сообщения")
	commitIntervalFlag := flag.Int("commit-interval", 1, "Интервал подтверждения сообщений (в секундах)")
	flag.Parse()

	// Настраиваем логгер
	log := app.SetupLogger()

	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	// Используем тему из аргументов командной строки или из конфигурации
	topic := *topicFlag
	if topic == "" {
		topic = cfg.Kafka.TopicName
	}

	// Создаем контекст с обработкой сигналов
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("Запуск потребителя ключей и значений...")

	// Создаем конфигурацию Sarama
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Version = sarama.V2_1_0_0

	// Настройка автоматического подтверждения
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = *autoCommitFlag
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = time.Duration(*commitIntervalFlag) * time.Second

	// Настройка начального смещения
	if *fromBeginningFlag {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Создаем группу потребителей
	log.Infof("Создаем группу потребителей %s для темы %s", *groupFlag, topic)
	log.Infof("Автоподтверждение: %v, интервал подтверждения: %d сек", *autoCommitFlag, *commitIntervalFlag)

	consumerGroup, err := sarama.NewConsumerGroup([]string{cfg.Kafka.Broker}, *groupFlag, saramaConfig)
	if err != nil {
		log.Fatalf("Ошибка создания группы потребителей: %v", err)
	}
	defer consumerGroup.Close()

	// Создаем обработчик
	handler := &KeyValueHandler{
		log:         log,
		readySignal: make(chan bool),
		autoCommit:  *autoCommitFlag,
	}

	// Обработка ошибок группы потребителей
	go func() {
		for err := range consumerGroup.Errors() {
			log.Errorf("Ошибка группы потребителей: %v", err)
		}
	}()

	// Запускаем потребление сообщений в отдельной горутине
	go func() {
		for {
			log.Infof("Начинаем потребление из темы %s", topic)

			if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
				log.Errorf("Ошибка при потреблении: %v", err)
			}

			// Проверяем контекст
			if ctx.Err() != nil {
				return
			}

			// Небольшая пауза перед повторной попыткой
			time.Sleep(time.Second)
		}
	}()

	// Ожидаем сигнал готовности
	<-handler.readySignal
	log.Info("Потребитель готов к приему сообщений...")

	// Ожидаем сигнал завершения
	<-ctx.Done()
	log.Info("Потребитель завершает работу...")
}

// KeyValueHandler обрабатывает сообщения, выводя ключи и значения
type KeyValueHandler struct {
	log          *logrus.Logger
	readySignal  chan bool
	ready        bool
	autoCommit   bool
	messageCount int
}

func (h *KeyValueHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.log.Infof("Настройка сессии для разделов: %v", session.Claims())
	h.messageCount = 0

	// Отправляем сигнал готовности только один раз
	if !h.ready {
		close(h.readySignal)
		h.ready = true
	}

	return nil
}

func (h *KeyValueHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.log.Infof("Очистка сессии. Обработано сообщений: %d", h.messageCount)
	return nil
}

func (h *KeyValueHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.log.Infof("Начинаем обработку раздела %d темы %s", claim.Partition(), claim.Topic())

	for message := range claim.Messages() {
		h.messageCount++

		// Получаем ключ и значение
		key := "<nil>"
		if message.Key != nil {
			key = string(message.Key)
		}

		value := "<nil>"
		if message.Value != nil {
			value = string(message.Value)
		}

		// Выводим информацию о сообщении
		fmt.Println("\n===============================================")
		fmt.Printf("КЛЮЧ: %s\n", key)
		fmt.Printf("ЗНАЧЕНИЕ: %s\n", value)
		fmt.Printf("ТЕМА: %s\n", message.Topic)
		fmt.Printf("РАЗДЕЛ: %d\n", message.Partition)
		fmt.Printf("СМЕЩЕНИЕ: %d\n", message.Offset)
		fmt.Printf("МЕТКА ВРЕМЕНИ: %v\n", message.Timestamp)

		// Подтверждаем получение сообщения
		if h.autoCommit {
			session.MarkMessage(message, "")
			fmt.Println("СТАТУС: Сообщение подтверждено ✓")
		} else {
			fmt.Println("СТАТУС: Сообщение не подтверждено (автоподтверждение выключено)")
		}

		fmt.Println("===============================================")

		// Периодически выводим статистику
		if h.messageCount%10 == 0 {
			h.log.Infof("Обработано сообщений: %d (последнее смещение: %d, раздел: %d)",
				h.messageCount, message.Offset, message.Partition)
		}
	}

	return nil
}
