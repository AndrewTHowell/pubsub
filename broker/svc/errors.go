package svc

import (
	"fmt"
)

type errTopicNotFound struct {
	topic string
}

func (e errTopicNotFound) Error() string {
	return fmt.Sprintf("topic %q not found", e.topic)
}

type errSubscriberNotFound struct {
	subscriberID string
}

func (e errSubscriberNotFound) Error() string {
	return fmt.Sprintf("subscriber %q not found", e.subscriberID)
}

type errInvalidOffsetDelta struct {
	delta int
}

func (e errInvalidOffsetDelta) Error() string {
	return fmt.Sprintf("offset delta %d is larger than the topic length", e.delta)
}
