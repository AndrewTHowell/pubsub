package broker

func New(topics ...string) Broker {
	return Broker{messagesByTopic: make(map[string][]Message, len(topics))}
}

type Broker struct {
	messagesByTopic map[string][]Message
}

func (b Broker) Publish(topic string, message Message) error {
	return nil
}

func (b Broker) Subscribe(topic string) (<-chan Message, error) {
	return nil, nil
}

type Message struct {
	Payload any
}
