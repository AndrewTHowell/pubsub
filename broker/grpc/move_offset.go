package grpc

import (
	"context"
	"fmt"

	errdetailspb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/emptypb"

	brokerpb "pubsub/broker/proto/broker"
	grpcerrors "pubsub/common/grpc/errors"
)

func (s Server) MoveOffset(ctx context.Context, request *brokerpb.MoveOffsetRequest) (*emptypb.Empty, error) {
	if err := s.validateMoveOffsetRequest(request); err != nil {
		return nil, fmt.Errorf("moving offset: %w", err)
	}

	if err := s.svc.MoveOffset(request.GetTopic(), request.GetGroup(), int(request.GetDelta())); err != nil {
		return nil, fmt.Errorf("moving offset: %w", err)
	}
	return nil, nil
}

func (Server) validateMoveOffsetRequest(request *brokerpb.MoveOffsetRequest) error {
	violations := []*errdetailspb.BadRequest_FieldViolation{}
	if !request.HasTopic() {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:  "topic",
			Reason: "REQUIRED_FIELD",
		})
	}
	if !request.HasGroup() {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:  "group",
			Reason: "REQUIRED_FIELD",
		})
	}
	if !request.HasDelta() {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:  "limit",
			Reason: "REQUIRED_FIELD",
		})
	} else if request.GetDelta() < 0 {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:       "delta",
			Reason:      "BELOW_MIN_VALUE",
			Description: "Minimum value 0",
		})
	}

	if len(violations) != 0 {
		return grpcerrors.NewInvalidArgument("invalid move offset request", violations...)
	}
	return nil
}
