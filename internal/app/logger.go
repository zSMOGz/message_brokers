package app

import "github.com/sirupsen/logrus"

var log = logrus.New()

func SetupLogger() *logrus.Logger {
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.DebugLevel)

	return log
}
