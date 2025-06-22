package broker

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

func (b *Broker) Publish(topic string, message Message) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	messages, ok := b.messagesByTopic[topic]
	if !ok {
		return errTopicNotFound{topic: topic}
	}

	*messages = append(*messages, message)
	return nil
}

func (b *Broker) Subscribe(topic, group string, maxBufferSize int) (Poller, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if _, ok := b.messagesByTopic[topic]; !ok {
		// Check at subscribe time.
		return nil, errTopicNotFound{topic: topic}
	}
	if _, ok := b.messagesByTopic[topic]; !ok {
		// Check at subscribe time.
		log.Panicf("topic %q present in messagesByTopic but not in offsetByGroup\n", topic)
	}
	return b.poller(topic, group, maxBufferSize), nil
}

func (b *Broker) poller(topic, group string, maxBufferSize int) Poller {
	return func() ([]Message, error) {
		b.mutex.Lock()
		defer b.mutex.Unlock()

		// Caller guarantees that these keys are present.
		messages := b.messagesByTopic[topic]
		offsetByGroup := b.offsetByGroupByTopic[topic]

		offset := offsetByGroup[group]
		if offset == len(*messages) {
			// Group has polled all messages.
			return nil, nil
		}

		newOffset := min(offset+maxBufferSize, len(*messages))

		messagesToPoll := *messages
		messagesToPoll = messagesToPoll[offset:newOffset]

		offsetByGroup[group] = newOffset
		return messagesToPoll, nil
	}
}

type Poller func() ([]Message, error)

type Message struct {
	Payload any
}
