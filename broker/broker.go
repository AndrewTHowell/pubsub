package broker

import "fmt"

type errTopicNotFound struct {
	topic string
}

func (e errTopicNotFound) Error() string {
	return fmt.Sprintf("topic %q not found", e.topic)
}

func New(bufferSize int, topics ...string) Broker {
	messageChanByTopic := make(map[string]chan Message, len(topics))
	for _, topic := range topics {
		messageChanByTopic[topic] = make(chan Message, bufferSize)
	}
	return Broker{messageChanByTopic: messageChanByTopic}
}

type Broker struct {
	messageChanByTopic map[string]chan Message
}

func (b Broker) Publish(topic string, message Message) error {
	messageChan, ok := b.messageChanByTopic[topic]
	if !ok {
		return errTopicNotFound{topic: topic}
	}

	messageChan <- message
	return nil
}

func (b Broker) Subscribe(topic string) (<-chan Message, error) {
	messageChan, ok := b.messageChanByTopic[topic]
	if !ok {
		return nil, errTopicNotFound{topic: topic}
	}
	return messageChan, nil
}

type Message struct {
	Payload any
}
