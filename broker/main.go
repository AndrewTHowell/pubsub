package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	brokergrpc "pubsub/broker/grpc"
	brokerpb "pubsub/broker/proto/broker"
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 9123))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	brokerpb.RegisterBrokerServer(srv, brokergrpc.NewServer())

	log.Println("Starting Broker.")
	srv.Serve(lis)
}
