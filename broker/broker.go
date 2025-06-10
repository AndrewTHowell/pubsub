package broker

import "fmt"

type errTopicNotFound struct {
	topic string
}

func (e errTopicNotFound) Error() string {
	return fmt.Sprintf("topic %q not found", e.topic)
}

func New(topics ...string) Broker {
	return Broker{messagesByTopic: make(map[string][]Message, len(topics))}
}

type Broker struct {
	messagesByTopic map[string][]Message
}

func (b Broker) Publish(topic string, message Message) error {
	if _, ok := b.messagesByTopic[topic]; !ok {
		return errTopicNotFound{topic: topic}
	}
	return nil
}

func (b Broker) Subscribe(topic string) (<-chan Message, error) {
	if _, ok := b.messagesByTopic[topic]; !ok {
		return nil, errTopicNotFound{topic: topic}
	}
	return nil, nil
}

type Message struct {
	Payload any
}
