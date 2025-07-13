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
	"pubsub/common/logging"
)

type Config struct {
	Logging logging.Config `koanf:"logging"`
	Port    int            `koanf:"port"`
	Topics  []Topic        `koanf:"topics"`
}

type Topic struct {
	Name               string `koanf:"name"`
	NumberOfPartitions int    `koanf:"num_of_partitions"`
}

func main() {
	cfg, err := config.ParseYAML[Config]("broker/config.yml", "config")
	if err != nil {
		slog.Error("Parsing config", slog.Any("error", err))
		os.Exit(1)
	}
	logging.SetLevel(cfg.Logging)
	slog.Debug("Config loaded", slog.Any("config", cfg))

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Port))
	if err != nil {
		slog.Error("Failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcerrors.UnaryServerInterceptor))
	topics := []string{}
	for _, t := range cfg.Topics {
		topics = append(topics, t.Name)
	}
	brokerpb.RegisterBrokerServer(srv, brokergrpc.NewServer(topics...))

	log.Printf("Starting Broker, listening on port %d.\n", cfg.Port)
	if srv.Serve(lis); err != nil {
		slog.Error("Server exited", slog.Any("error", err))
		os.Exit(1)
	}
}
