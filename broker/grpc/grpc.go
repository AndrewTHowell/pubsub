package grpc

import (
	brokerpb "pubsub/broker/proto/broker"
	"pubsub/broker/svc"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	brokerpb.UnimplementedBrokerServer

	svc svc.Broker
}

type Topic struct {
	Name               string
	NumberOfPartitions int
}

func NewServer(topics ...Topic) Server {
	svcTopics := make([]svc.TopicDefinition, 0, len(topics))
	for _, t := range topics {
		svcTopics = append(svcTopics, svc.TopicDefinition{
			Name:               t.Name,
			NumberOfPartitions: t.NumberOfPartitions,
		})
	}
	return Server{
		svc: svc.NewBroker(svcTopics...),
	}
}

func (Server) convertToMessages(protoMessages ...*brokerpb.Message) []svc.Message {
	messages := make([]svc.Message, len(protoMessages))
	for i, protoMessage := range protoMessages {
		messages[i] = svc.Message{
			Key:       protoMessage.GetKey(),
			Timestamp: protoMessage.GetTimestamp().AsTime(),
			Payload:   protoMessage.GetPayload(),
		}
	}
	return messages
}

func (Server) convertFromMessages(messages ...svc.Message) []*brokerpb.Message {
	protoMessages := make([]*brokerpb.Message, len(messages))
	for i, message := range messages {
		protoMessages[i] = brokerpb.Message_builder{
			Key:       &message.Key,
			Timestamp: timestamppb.New(message.Timestamp),
			Payload:   message.Payload,
		}.Build()
	}
	return protoMessages
}
