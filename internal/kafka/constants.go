package kafka

import "time"

const (
	MessageCount      = 10
	DefaultPartition  = 0
	DefaultRetryCount = 3
	DefaultRetryDelay = time.Second
)
