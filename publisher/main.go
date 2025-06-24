package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	brokerpb "pubsub/broker/proto/broker"
	"pubsub/common/config"
)

type Config struct {
	Port int `koanf:"port"`
}

func main() {
	cfg, err := config.ParseYAML[Config]("publisher/config.yml", "config")
	if err != nil {
		log.Fatalf("parsing config: %v", err)
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", cfg.Port), opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := brokerpb.NewBrokerClient(conn)

	request := brokerpb.PublishRequest_builder{
		Topic: toPtr("animals.cats"),
		Messages: []*brokerpb.Message{brokerpb.Message_builder{
			Payload: []byte("meow"),
		}.Build()},
	}.Build()
	if _, err := client.Publish(context.Background(), request); err != nil {
		log.Fatalf("publishing: %v", err)
	}
}

func toPtr[P *T, T any](t T) P { return &t }
