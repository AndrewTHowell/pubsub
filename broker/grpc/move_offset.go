package grpc

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	brokerpb "pubsub/broker/proto/broker"
	commonerrors "pubsub/common/errors"
)

func (s Server) MoveOffset(ctx context.Context, request *brokerpb.MoveOffsetRequest) (*emptypb.Empty, error) {
	if err := s.validateMoveOffsetRequest(request); err != nil {
		return nil, fmt.Errorf("moving offset: %w", err)
	}

	if err := s.svc.MoveOffset(request.GetSubscriberId(), int(request.GetDelta())); err != nil {
		return nil, fmt.Errorf("moving offset: %w", err)
	}
	return nil, nil
}

func (Server) validateMoveOffsetRequest(request *brokerpb.MoveOffsetRequest) error {
	violations := []commonerrors.FieldViolation{}
	if !request.HasSubscriberId() {
		violations = append(violations, commonerrors.FieldViolation{
			Field:  "subscriber_id",
			Reason: "REQUIRED_FIELD",
		})
	}
	if !request.HasDelta() {
		violations = append(violations, commonerrors.FieldViolation{
			Field:  "limit",
			Reason: "REQUIRED_FIELD",
		})
	} else if request.GetDelta() < 1 {
		violations = append(violations, commonerrors.FieldViolation{
			Field:       "delta",
			Reason:      "BELOW_MIN_VALUE",
			Description: "Minimum value 1",
		})
	}

	if len(violations) != 0 {
		return commonerrors.NewInvalidArgument("invalid move offset request", violations...)
	}
	return nil
}
