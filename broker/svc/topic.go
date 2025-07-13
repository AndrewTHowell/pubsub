package svc

import (
	"fmt"
	"sync"
)

type TopicDefinition struct {
	Name               string
	NumberOfPartitions int
}

type Topic struct {
	mutex         sync.RWMutex
	messages      *[]Message
	offsetByGroup map[string]int
}

func newTopic(numberOfPartitions int) *Topic {
	return &Topic{
		messages:      &[]Message{},
		offsetByGroup: map[string]int{},
	}
}

func (t *Topic) publish(newMessages ...Message) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	*t.messages = append(*t.messages, newMessages...)
	return nil
}

func (t *Topic) subscribe(group string, maxBufferSize int) Poller {
	return func() ([]Message, error) {
		messages, err := t.poll(group, maxBufferSize)
		if err != nil {
			return nil, err
		}

		if err := t.moveOffset(group, len(messages)); err != nil {
			return nil, err
		}

		return messages, nil
	}
}

func (t *Topic) poll(group string, maxBufferSize int) ([]Message, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	offset := t.offsetByGroup[group]
	if offset == len(*t.messages) {
		// Group has polled all messages.
		return nil, nil
	}

	end := min(offset+maxBufferSize, len(*t.messages))

	polledMessages := *t.messages
	polledMessages = polledMessages[offset:end]

	return polledMessages, nil
}

func (t *Topic) moveOffset(group string, delta int) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	newOffset := t.offsetByGroup[group] + delta
	if newOffset > len(*t.messages) {
		return fmt.Errorf("committing: %w", errInvalidOffsetDelta{delta: delta})
	}
	t.offsetByGroup[group] = newOffset

	return nil
}
