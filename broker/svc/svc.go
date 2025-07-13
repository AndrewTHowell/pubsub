package svc

import (
	"time"
)

type Message struct {
	Key string
	// When the Message was first processed by the Broker.
	Timestamp time.Time
	Payload   []byte
}
