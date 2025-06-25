package grpc

import (
	"context"
	"fmt"

	brokerpb "pubsub/broker/proto/broker"
	grpcerrors "pubsub/common/grpc/errors"

	errdetailspb "google.golang.org/genproto/googleapis/rpc/errdetails"
)

func (s Server) Poll(ctx context.Context, request *brokerpb.PollRequest) (*brokerpb.PollResponse, error) {
	if err := s.validatePollRequest(request); err != nil {
		return nil, fmt.Errorf("polling: %w", err)
	}

	messages, err := s.svc.Poll(request.GetTopic(), request.GetGroup(), int(request.GetLimit()))
	if err != nil {
		return nil, fmt.Errorf("polling: %w", err)
	}

	return brokerpb.PollResponse_builder{
		Messages: s.convertFromMessages(messages...),
	}.Build(), nil
}

func (Server) validatePollRequest(request *brokerpb.PollRequest) error {
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
	if !request.HasLimit() {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:  "limit",
			Reason: "REQUIRED_FIELD",
		})
	} else if request.GetLimit() < 1 {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:       "limit",
			Reason:      "BELOW_MIN_VALUE",
			Description: "Minimum value 1",
		})
	}

	if len(violations) != 0 {
		return grpcerrors.NewInvalidArgument("invalid poll request", violations...)
	}
	return nil
}
