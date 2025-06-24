package grpc

import (
	"context"
	"errors"
	"fmt"

	brokerpb "pubsub/broker/proto/broker"
)

func (s Server) Poll(ctx context.Context, request *brokerpb.PollRequest) (*brokerpb.PollResponse, error) {
	if err := s.validatePollRequest(request); err != nil {
		return nil, fmt.Errorf("polling : %w", err)
	}

	messages, err := s.svc.Poll(request.GetTopic(), request.GetGroup(), int(request.GetLimit()))
	if err != nil {
		return nil, fmt.Errorf("polling : %w", err)
	}

	return brokerpb.PollResponse_builder{
		Messages: s.convertFromMessages(messages...),
	}.Build(), nil
}

func (Server) validatePollRequest(request *brokerpb.PollRequest) error {
	var err error
	if !request.HasTopic() {
		err = errors.Join(err, fmt.Errorf("'topic' field required"))
	}
	if !request.HasGroup() {
		err = errors.Join(err, fmt.Errorf("'group' field required"))
	}
	if !request.HasLimit() {
		err = errors.Join(err, fmt.Errorf("'limit' field required"))
	}
	return err
}
