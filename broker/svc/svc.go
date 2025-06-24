package svc

import (
	"fmt"
	"log"
	"sync"
)

type errTopicNotFound struct {
	topic string
}

func (e errTopicNotFound) Error() string {
	return fmt.Sprintf("topic %q not found", e.topic)
}

func New(topics ...string) *Broker {
	messagesByTopic := make(map[string]*[]Message, len(topics))
	offsetByGroupByTopic := make(map[string]map[string]int, len(topics))
	for _, topic := range topics {
		messagesByTopic[topic] = &[]Message{}
		offsetByGroupByTopic[topic] = map[string]int{}
	}
	return &Broker{
		messagesByTopic:      messagesByTopic,
		offsetByGroupByTopic: offsetByGroupByTopic,
	}
}

type Broker struct {
	mutex                sync.RWMutex
	messagesByTopic      map[string]*[]Message
	offsetByGroupByTopic map[string]map[string]int
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
		log.Panicf("topic %q present in messagesByTopic but not in offsetByGroup\n", topic)
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

func (b *Broker) MoveOffset(topic, group string, offset int) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	offsetByGroup, ok := b.offsetByGroupByTopic[topic]
	if !ok {
		return fmt.Errorf("committing: %w", errTopicNotFound{topic: topic})
	}
	offsetByGroup[group] = offsetByGroup[group] + offset

	return nil
}

type Poller func() ([]Message, error)

type Message struct {
	Payload []byte
}
