package svc

import (
	"errors"
	"fmt"

	commonerrors "pubsub/common/errors"
)

type Broker struct {
	topicsByName            map[string]*Topic
	topicNameBySubscriberID map[string]string
}

func NewBroker(topicDefs ...TopicDefinition) Broker {
	topicsByName := make(map[string]*Topic, len(topicDefs))
	var errs error
	for _, topicDef := range topicDefs {
		topic, err := newTopic(topicDef.Name, topicDef.NumberOfPartitions)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("invalid topic definition for topic %q: %w", topicDef.Name, err))
		}
		topicsByName[topicDef.Name] = topic
	}
	return Broker{
		topicsByName:            topicsByName,
		topicNameBySubscriberID: map[string]string{},
	}
}

func (b Broker) Publish(topicName string, newMessages ...Message) error {
	topic, ok := b.topicsByName[topicName]
	if !ok {
		return errTopicNotFound{topic: topicName}
	}
	return topic.publish(newMessages...)
}

func (b Broker) Subscribe(topicName, group string) (string, error) {
	topic, ok := b.topicsByName[topicName]
	if !ok {
		return "", errTopicNotFound{topic: topicName}
	}

	subscriberID := topic.subscribe(group)
	b.topicNameBySubscriberID[subscriberID] = topicName

	return subscriberID, nil
}

func (b Broker) Poll(subscriberID string, maxBufferSize int) ([]Message, error) {
	topicName, ok := b.topicNameBySubscriberID[subscriberID]
	if !ok {
		return nil, commonerrors.NewFailedPrecondition("invalid polling request", commonerrors.PreconditionFailure{
			Type:        errSubscriberNotFound,
			Description: fmt.Sprintf("Subscriber %q is not a registered subscriber of the broker.", subscriberID),
		})
	}
	topic, ok := b.topicsByName[topicName]
	if !ok {
		// This shouldn't happen, as the subscriber was found, meaning the topic existed in the past.
		return nil, errTopicNotFound{topic: topicName}
	}
	return topic.poll(subscriberID, maxBufferSize)
}

func (b Broker) MoveOffset(subscriberID string, delta int) error {
	topicName, ok := b.topicNameBySubscriberID[subscriberID]
	if !ok {
		return commonerrors.NewFailedPrecondition("invalid polling request", commonerrors.PreconditionFailure{
			Type:        errSubscriberNotFound,
			Description: fmt.Sprintf("Subscriber %q is not a registered subscriber of the broker.", subscriberID),
		})
	}
	topic, ok := b.topicsByName[topicName]
	if !ok {
		// This shouldn't happen, as the subscriber was found, meaning the topic existed in the past.
		return errTopicNotFound{topic: topicName}
	}
	return topic.moveOffset(subscriberID, delta)
}
