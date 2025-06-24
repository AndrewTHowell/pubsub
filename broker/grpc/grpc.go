package grpc

import (
	brokerpb "pubsub/broker/proto/broker"
	"pubsub/broker/svc"
)

type Server struct {
	brokerpb.UnimplementedBrokerServer

	svc *svc.Broker
}

func NewServer(topics ...string) Server {
	return Server{
		svc: svc.New(topics...),
	}
}

func (Server) convertToMessages(protoMessages ...*brokerpb.Message) []svc.Message {
	messages := make([]svc.Message, len(protoMessages))
	for i, protoMessage := range protoMessages {
		messages[i] = svc.Message{
			Payload: protoMessage.GetPayload(),
		}
	}
	return messages
}

func (Server) convertFromMessages(messages ...svc.Message) []*brokerpb.Message {
	protoMessages := make([]*brokerpb.Message, len(messages))
	for i, message := range messages {
		protoMessages[i] = brokerpb.Message_builder{
			Payload: message.Payload,
		}.Build()
	}
	return protoMessages
}
