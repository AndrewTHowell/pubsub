package grpc

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	brokerpb "pubsub/broker/proto/broker"
	commonerrors "pubsub/common/errors"
)

func (s Server) Publish(ctx context.Context, request *brokerpb.PublishRequest) (*emptypb.Empty, error) {
	if err := s.validatePublishRequest(request); err != nil {
		return nil, fmt.Errorf("publishing: %w", err)
	}

	if err := s.svc.Publish(request.GetTopic(), s.convertToMessages(request.GetMessages()...)...); err != nil {
		return nil, fmt.Errorf("publishing: %w", err)
	}
	return nil, nil
}

func (Server) validatePublishRequest(request *brokerpb.PublishRequest) error {
	violations := []commonerrors.FieldViolation{}
	if !request.HasTopic() {
		violations = append(violations, commonerrors.FieldViolation{
			Field:  "topic",
			Reason: "REQUIRED_FIELD",
		})
	}
	if len(request.GetMessages()) == 0 {
		violations = append(violations, commonerrors.FieldViolation{
			Field:       "messages",
			Reason:      "BELOW_MIN_LENGTH",
			Description: "Minimum length 1",
		})
	}
	for i, msg := range request.GetMessages() {
		if !msg.HasPayload() {
			violations = append(violations, commonerrors.FieldViolation{
				Field:  fmt.Sprintf("messages[%d].payload", i),
				Reason: "REQUIRED_FIELD",
			})
		}
	}

	if len(violations) != 0 {
		return commonerrors.NewInvalidArgument("invalid publish request", violations...)
	}
	return nil
}
