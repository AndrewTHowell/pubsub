package broker

import (
	"fmt"
)

type errTopicNotFound struct {
	topic string
}

func (e errTopicNotFound) Error() string {
	return fmt.Sprintf("topic %q not found", e.topic)
}

func New(topics ...string) Broker {
	messagesByTopic := make(map[string]*[]Message, len(topics))
	for _, topic := range topics {
		messagesByTopic[topic] = &[]Message{}
	}
	return Broker{
		messagesByTopic: messagesByTopic,
		offsetByGroup:   map[string]int{},
	}
}

type Broker struct {
	messagesByTopic map[string]*[]Message
	offsetByGroup   map[string]int
}

func (b Broker) Publish(topic string, message Message) error {
	messages, ok := b.messagesByTopic[topic]
	if !ok {
		return errTopicNotFound{topic: topic}
	}

	*messages = append(*messages, message)
	return nil
}

func (b Broker) Subscribe(topic, group string, maxBufferSize int) (Poller, error) {
	messages, ok := b.messagesByTopic[topic]
	if !ok {
		return nil, errTopicNotFound{topic: topic}
	}
	return b.poller(group, maxBufferSize, messages), nil
}

func (b Broker) poller(group string, maxBufferSize int, messages *[]Message) Poller {
	return func() ([]Message, error) {
		offset := b.offsetByGroup[group]
		if offset == len(*messages) {
			// Group has polled all messages.
			return nil, nil
		}

		newOffset := min(offset+maxBufferSize, len(*messages))

		messagesToPoll := *messages
		messagesToPoll = messagesToPoll[offset:newOffset]

		b.offsetByGroup[group] = newOffset
		return messagesToPoll, nil
	}
}

type Poller func() ([]Message, error)

type Message struct {
	Payload any
}
