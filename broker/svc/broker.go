package svc

type Broker struct {
	topicsByName map[string]*Topic
}

func NewBroker(topics ...TopicDefinition) Broker {
	topicsByName := make(map[string]*Topic, len(topics))
	for _, topic := range topics {
		topicsByName[topic.Name] = newTopic(topic.NumberOfPartitions)
	}
	return Broker{
		topicsByName: topicsByName,
	}
}

func (b Broker) Publish(topicName string, newMessages ...Message) error {
	topic, ok := b.topicsByName[topicName]
	if !ok {
		return errTopicNotFound{topic: topicName}
	}
	return topic.publish(newMessages...)
}

func (b Broker) Subscribe(topicName, group string, maxBufferSize int) (Poller, error) {
	topic, ok := b.topicsByName[topicName]
	if !ok {
		return nil, errTopicNotFound{topic: topicName}
	}
	return topic.subscribe(group, maxBufferSize), nil
}

func (b Broker) Poll(topicName, group string, maxBufferSize int) ([]Message, error) {
	topic, ok := b.topicsByName[topicName]
	if !ok {
		return nil, errTopicNotFound{topic: topicName}
	}
	return topic.poll(group, maxBufferSize)
}

func (b Broker) MoveOffset(topicName, group string, delta int) error {
	topic, ok := b.topicsByName[topicName]
	if !ok {
		return errTopicNotFound{topic: topicName}
	}
	return topic.moveOffset(group, delta)
}
