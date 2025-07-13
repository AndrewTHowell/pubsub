package svc

import (
	"fmt"
)

var errSubscriberNotFound = "SUBSCRIBER_NOT_FOUND"

type errTopicNotFound struct {
	topic string
}

func (e errTopicNotFound) Error() string {
	return fmt.Sprintf("topic %q not found", e.topic)
}

type errInvalidOffsetDelta struct {
	delta int
}

func (e errInvalidOffsetDelta) Error() string {
	return fmt.Sprintf("offset delta %d is larger than the topic length", e.delta)
}
