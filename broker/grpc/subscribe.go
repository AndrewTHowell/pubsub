package grpc

import (
	"context"
	"fmt"

	brokerpb "pubsub/broker/proto/broker"
	commonerrors "pubsub/common/errors"
)

func (s Server) Subscribe(ctx context.Context, request *brokerpb.SubscribeRequest) (*brokerpb.SubscribeResponse, error) {
	if err := s.validateSubscribeRequest(request); err != nil {
		return nil, fmt.Errorf("subscribing: %w", err)
	}

	subscriberID, err := s.svc.Subscribe(request.GetTopic(), request.GetGroup())
	if err != nil {
		return nil, fmt.Errorf("subscribing: %w", err)
	}

	return brokerpb.SubscribeResponse_builder{
		SubscriberId: &subscriberID,
	}.Build(), nil
}

func (Server) validateSubscribeRequest(request *brokerpb.SubscribeRequest) error {
	violations := []commonerrors.FieldViolation{}
	if !request.HasTopic() {
		violations = append(violations, commonerrors.FieldViolation{
			Field:  "topic",
			Reason: "REQUIRED_FIELD",
		})
	}
	if !request.HasGroup() {
		violations = append(violations, commonerrors.FieldViolation{
			Field:  "group",
			Reason: "REQUIRED_FIELD",
		})
	}

	if len(violations) != 0 {
		return commonerrors.NewInvalidArgument("invalid subscribe request", violations...)
	}
	return nil
}
