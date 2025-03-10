package app

import (
	"context"
	"mb/internal/config"
	"mb/internal/kafka"
	"net/http"

	"github.com/heptiolabs/healthcheck"
)

func SetupHealthCheck(cfg *config.Config) healthcheck.Handler {
	health := healthcheck.NewHandler()
	health.AddReadinessCheck("kafka", func() error {
		return kafka.WaitForBroker(context.Background(), cfg.Kafka.Broker, 1)
	})
	return health
}

func StartHealthServer(health healthcheck.Handler) {
	go func() {
		http.ListenAndServe(":8086", health)
	}()
}
