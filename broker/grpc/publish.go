package grpc

import (
	"context"
	"fmt"

	errdetailspb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	brokerpb "pubsub/broker/proto/broker"
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
	violations := []*errdetailspb.BadRequest_FieldViolation{}
	if !request.HasTopic() {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:  "topic",
			Reason: "REQUIRED_FIELD",
		})
	}
	if len(request.GetMessages()) == 0 {
		violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
			Field:       "messages",
			Reason:      "BELOW_MIN_LENGTH",
			Description: "Minimum length 1",
		})
	}
	for i, msg := range request.GetMessages() {
		if !msg.HasPayload() {
			violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
				Field:  fmt.Sprintf("messages[%d].payload", i),
				Reason: "REQUIRED_FIELD",
			})
		}
	}
	if len(violations) != 0 {
		st := status.New(codes.InvalidArgument, "invalid publish request")
		dst, err := st.WithDetails(&errdetailspb.BadRequest{FieldViolations: violations})
		if err != nil {
			return st.Err()
		}
		return dst.Err()
	}
	return nil
}
