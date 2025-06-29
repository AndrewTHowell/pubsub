package main

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"

	brokergrpc "pubsub/broker/grpc"
	brokerpb "pubsub/broker/proto/broker"
	"pubsub/common/config"
	grpcerrors "pubsub/common/grpc/errors"
)

type Config struct {
	Port   int      `koanf:"port"`
	Topics []string `koanf:"topics"`
}

func main() {
	cfg, err := config.ParseYAML[Config]("broker/config.yml", "config")
	if err != nil {
		slog.Error("Parsing config", slog.Any("error", err))
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Port))
	if err != nil {
		slog.Error("Failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcerrors.UnaryServerInterceptor))
	brokerpb.RegisterBrokerServer(srv, brokergrpc.NewServer(cfg.Topics...))

	log.Printf("Starting Broker, listening on port %d.\n", cfg.Port)
	if srv.Serve(lis); err != nil {
		slog.Error("Server exited", slog.Any("error", err))
		os.Exit(1)
	}
}
