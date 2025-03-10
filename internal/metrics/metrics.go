package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	messagesProducedName = "messages_produced_total"
	messagesConsumedName = "messages_consumed_total"
	processingTimeName   = "message_processing_time_seconds"
)

var (
	MessagesProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: messagesProducedName,
		Help: "Total number of produced messages",
	})
	MessagesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: messagesConsumedName,
		Help: "Total number of consumed messages",
	})
	MessageProcessingTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    processingTimeName,
		Help:    "Time spent processing messages",
		Buckets: prometheus.DefBuckets,
	})
)

func Init() {
	prometheus.MustRegister(MessagesProduced)
	prometheus.MustRegister(MessagesConsumed)
	prometheus.MustRegister(MessageProcessingTime)
}
