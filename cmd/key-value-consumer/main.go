package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"

	"mb/internal/app"
)

func main() {
	log, cfg, ctx, stop := app.InitApp()
	defer stop()

	// Парсим аргументы командной строки
	topicFlag := flag.String("topic", "", "Имя темы Kafka (если не указано, используется из конфига)")
	groupFlag := flag.String("group", "key-value-consumer-group", "Имя группы потребителей")
	fromBeginningFlag := flag.Bool("from-beginning", true, "Читать сообщения с начала")

	// Параметры для автоматического коммита
	autoCommitFlag := flag.Bool("auto-commit", true, "Автоматически подтверждать сообщения")
	commitIntervalFlag := flag.Int("commit-interval", 1, "Интервал автоматического подтверждения (в секундах)")
	manualMarkFlag := flag.Bool("manual-mark", false, "Вручную отмечать сообщения как прочитанные")
	flag.Parse()

	// Используем тему из аргументов командной строки или из конфигурации
	topic := *topicFlag
	if topic == "" {
		topic = cfg.Kafka.TopicName
	}

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

	// Включение расширенного логирования состояния группы
	saramaConfig.Consumer.Group.Session.Timeout = 20 * time.Second
	saramaConfig.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	// Создаем группу потребителей
	log.Infof("Создаем группу потребителей %s для темы %s", *groupFlag, topic)
	log.Infof("Режим коммита: автоматический=%v, интервал=%d сек, ручная отметка=%v",
		*autoCommitFlag, *commitIntervalFlag, *manualMarkFlag)

	consumerGroup, err := sarama.NewConsumerGroup([]string{cfg.Kafka.Broker}, *groupFlag, saramaConfig)
	if err != nil {
		log.Fatalf("Ошибка создания группы потребителей: %v", err)
	}
	defer consumerGroup.Close()

	// Создаем обработчик
	handler := &KeyValueHandler{
		log:            log,
		readySignal:    make(chan bool),
		autoCommit:     *autoCommitFlag,
		manualMark:     *manualMarkFlag,
		commitInterval: *commitIntervalFlag,
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
	log.Infof("Автоматический коммит будет выполняться каждые %d секунд", *commitIntervalFlag)

	// Ожидаем сигнал завершения
	<-ctx.Done()
	log.Info("Потребитель завершает работу...")
}

// KeyValueHandler обрабатывает сообщения, выводя ключи и значения
type KeyValueHandler struct {
	log            *logrus.Logger
	readySignal    chan bool
	ready          bool
	autoCommit     bool
	manualMark     bool
	commitInterval int
	messageCount   int
	lastCommitTime time.Time
}

func (h *KeyValueHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.log.Infof("Настройка сессии для разделов: %v", session.Claims())
	h.messageCount = 0
	h.lastCommitTime = time.Now()

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

	lastLoggedOffset := int64(-1)

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
		h.log.Println("\n===============================================")
		h.log.Printf("КЛЮЧ: %s\n", key)
		h.log.Printf("ЗНАЧЕНИЕ: %s\n", value)
		h.log.Printf("ТЕМА: %s\n", message.Topic)
		h.log.Printf("РАЗДЕЛ: %d\n", message.Partition)
		h.log.Printf("СМЕЩЕНИЕ: %d\n", message.Offset)
		h.log.Printf("МЕТКА ВРЕМЕНИ: %v\n", message.Timestamp)

		// Подтверждаем получение сообщения
		commitStatus := "Ожидание автоматического коммита"
		if h.manualMark {
			session.MarkMessage(message, "")
			commitStatus = "Сообщение отмечено для коммита"
		}

		// Определяем статус коммита
		if h.autoCommit {
			timeSinceCommit := time.Since(h.lastCommitTime).Seconds()
			timeToNextCommit := float64(h.commitInterval) - timeSinceCommit
			if timeToNextCommit < 0 {
				timeToNextCommit = 0
			}

			h.log.Printf("СТАТУС: %s (автокоммит через %.1f сек)\n",
				commitStatus, timeToNextCommit)
		} else {
			h.log.Println("СТАТУС: Автоматический коммит отключен")
		}

		fmt.Println("===============================================")

		// Выводим статистику при обработке 10 сообщений или изменении раздела
		if h.messageCount%10 == 0 || lastLoggedOffset == -1 || message.Partition != claim.Partition() {
			h.log.Infof("Обработано сообщений: %d (раздел: %d, смещение: %d)",
				h.messageCount, message.Partition, message.Offset)
			lastLoggedOffset = message.Offset
		}

		// Обновляем время последнего коммита, если прошел интервал коммита
		if h.autoCommit && time.Since(h.lastCommitTime) >= time.Duration(h.commitInterval)*time.Second {
			h.lastCommitTime = time.Now()
			h.log.Infof("Выполнен автоматический коммит смещений (раздел: %d, смещение: %d)",
				message.Partition, message.Offset)
		}
	}

	return nil
}
