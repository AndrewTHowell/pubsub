package grpc

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	brokerpb "pubsub/broker/proto/broker"
	"pubsub/broker/svc"
)

func (s Server) Publish(ctx context.Context, request *brokerpb.PublishRequest) (*emptypb.Empty, error) {
	if err := s.validatePublishRequest(request); err != nil {
		return nil, fmt.Errorf("publishing : %w", err)
	}

	if err := s.svc.Publish(request.GetTopic(), s.convertToMessages(request.GetMessages()...)...); err != nil {
		return nil, fmt.Errorf("publishing : %w", err)
	}
	return nil, nil
}

func (Server) validatePublishRequest(request *brokerpb.PublishRequest) error {
	var err error
	if !request.HasTopic() {
		err = errors.Join(err, fmt.Errorf("'topic' field required"))
	}
	if len(request.GetMessages()) == 0 {
		err = errors.Join(err, fmt.Errorf("'messages' field must be at least length 1"))
	}
	for i, msg := range request.GetMessages() {
		if !msg.HasPayload() {
			err = errors.Join(err, fmt.Errorf("'messages[%d].payload' field required", i))
		}
	}
	return err
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
