package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	brokergrpc "pubsub/broker/grpc"
	brokerpb "pubsub/broker/proto/broker"
	"pubsub/common/config"
)

type Config struct {
	Port   int      `koanf:"port"`
	Topics []string `koanf:"topics"`
}

func main() {
	cfg, err := config.ParseYAML[Config]("broker/config.yml", "config")
	if err != nil {
		log.Fatalf("parsing config: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	brokerpb.RegisterBrokerServer(srv, brokergrpc.NewServer(cfg.Topics...))

	log.Printf("Starting Broker, listening on port %d.\n", cfg.Port)
	if srv.Serve(lis); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
