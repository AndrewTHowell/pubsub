package grpc

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	brokerpb "pubsub/broker/proto/broker"
)

func (s Server) MoveOffset(ctx context.Context, request *brokerpb.MoveOffsetRequest) (*emptypb.Empty, error) {
	if err := s.validateMoveOffsetRequest(request); err != nil {
		return nil, fmt.Errorf("moving offset : %w", err)
	}

	if err := s.svc.MoveOffset(request.GetTopic(), request.GetGroup(), int(request.GetDelta())); err != nil {
		return nil, fmt.Errorf("moving offset : %w", err)
	}
	return nil, nil
}

func (Server) validateMoveOffsetRequest(request *brokerpb.MoveOffsetRequest) error {
	var err error
	if !request.HasTopic() {
		err = errors.Join(err, fmt.Errorf("'topic' field required"))
	}
	if !request.HasGroup() {
		err = errors.Join(err, fmt.Errorf("'group' field required"))
	}
	if !request.HasDelta() {
		err = errors.Join(err, fmt.Errorf("'delta' field required"))
	}
	return err
}
