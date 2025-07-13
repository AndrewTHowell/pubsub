package svc

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
)

type Broker struct {
	mutex                sync.RWMutex
	messagesByTopic      map[string]*[]Message
	offsetByGroupByTopic map[string]map[string]int
}

type Topic struct {
	Name               string
	NumberOfPartitions int
}

func New(topics ...Topic) *Broker {
	messagesByTopic := make(map[string]*[]Message, len(topics))
	offsetByGroupByTopic := make(map[string]map[string]int, len(topics))
	for _, topic := range topics {
		messagesByTopic[topic.Name] = &[]Message{}
		offsetByGroupByTopic[topic.Name] = map[string]int{}
	}
	return &Broker{
		messagesByTopic:      messagesByTopic,
		offsetByGroupByTopic: offsetByGroupByTopic,
	}
}

func (b *Broker) Publish(topic string, newMessages ...Message) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	messages, ok := b.messagesByTopic[topic]
	if !ok {
		return errTopicNotFound{topic: topic}
	}

	*messages = append(*messages, newMessages...)
	return nil
}

type Poller func() ([]Message, error)

func (b *Broker) Subscribe(topic, group string, maxBufferSize int) Poller {
	return func() ([]Message, error) {
		messages, err := b.Poll(topic, group, maxBufferSize)
		if err != nil {
			return nil, err
		}

		if err := b.MoveOffset(topic, group, len(messages)); err != nil {
			return nil, err
		}

		return messages, nil
	}
}

func (b *Broker) Poll(topic, group string, maxBufferSize int) ([]Message, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	messages, ok := b.messagesByTopic[topic]
	if !ok {
		return nil, fmt.Errorf("polling: %w", errTopicNotFound{topic: topic})
	}
	offsetByGroup, ok := b.offsetByGroupByTopic[topic]
	if !ok {
		slog.Error("Topic present in messagesByTopic but not in offsetByGroup", slog.String("topic", topic))
		os.Exit(1)
	}

	offset := offsetByGroup[group]
	if offset == len(*messages) {
		// Group has polled all messages.
		return nil, nil
	}

	end := min(offset+maxBufferSize, len(*messages))

	polledMessages := *messages
	polledMessages = polledMessages[offset:end]

	return polledMessages, nil
}

func (b *Broker) MoveOffset(topic, group string, delta int) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	messages, ok := b.messagesByTopic[topic]
	if !ok {
		return fmt.Errorf("committing: %w", errTopicNotFound{topic: topic})
	}
	offsetByGroup, ok := b.offsetByGroupByTopic[topic]
	if !ok {
		slog.Error("Topic present in messagesByTopic but not in offsetByGroup", slog.String("topic", topic))
		os.Exit(1)
	}

	newOffset := offsetByGroup[group] + delta
	if newOffset > len(*messages) {
		return fmt.Errorf("committing: %w", errInvalidOffsetDelta{delta: delta})
	}
	offsetByGroup[group] = newOffset

	return nil
}
