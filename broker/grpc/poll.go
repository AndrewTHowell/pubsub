package grpc

import (
	"context"
	"fmt"

	brokerpb "pubsub/broker/proto/broker"
	commonerrors "pubsub/common/errors"
)

func (s Server) Poll(ctx context.Context, request *brokerpb.PollRequest) (*brokerpb.PollResponse, error) {
	if err := s.validatePollRequest(request); err != nil {
		return nil, fmt.Errorf("polling: %w", err)
	}

	messages, err := s.svc.Poll(request.GetSubscriberId(), int(request.GetLimit()))
	if err != nil {
		return nil, fmt.Errorf("polling: %w", err)
	}

	return brokerpb.PollResponse_builder{
		Messages: s.convertFromMessages(messages...),
	}.Build(), nil
}

func (Server) validatePollRequest(request *brokerpb.PollRequest) error {
	violations := []commonerrors.FieldViolation{}
	if !request.HasSubscriberId() {
		violations = append(violations, commonerrors.FieldViolation{
			Field:  "subscriber_id",
			Reason: "REQUIRED_FIELD",
		})
	}
	if !request.HasLimit() {
		violations = append(violations, commonerrors.FieldViolation{
			Field:  "limit",
			Reason: "REQUIRED_FIELD",
		})
	} else if request.GetLimit() < 1 {
		violations = append(violations, commonerrors.FieldViolation{
			Field:       "limit",
			Reason:      "BELOW_MIN_VALUE",
			Description: "Minimum value 1",
		})
	}

	if len(violations) != 0 {
		return commonerrors.NewInvalidArgument("invalid poll request", violations...)
	}
	return nil
}
