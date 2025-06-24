package main

import (
	"fmt"
	"log"
	"net"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"google.golang.org/grpc"

	brokergrpc "pubsub/broker/grpc"
	brokerpb "pubsub/broker/proto/broker"
)

type config struct {
	Port   int      `koanf:"port"`
	Topics []string `koanf:"topics"`
}

// Global koanf instance. Use "." as the key path delimiter. This can be "/" or any character.
var k = koanf.New(".")

func main() {
	// Load YAML config.
	if err := k.Load(file.Provider("broker/config.yml"), yaml.Parser()); err != nil {
		log.Fatalf("loading config: %v", err)
	}
	var cfg config
	k.Unmarshal("config", &cfg)

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
