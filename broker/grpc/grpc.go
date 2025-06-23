package grpc

import (
	brokerpb "pubsub/broker/proto/broker"
	"pubsub/broker/svc"
)

type Server struct {
	brokerpb.UnimplementedBrokerServer

	svc *svc.Broker
}

func NewServer(topics ...string) Server {
	return Server{
		svc: svc.New(topics...),
	}
}
