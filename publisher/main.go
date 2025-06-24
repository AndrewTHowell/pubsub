package main

import (
	"context"
	"fmt"
	"log"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	brokerpb "pubsub/broker/proto/broker"
)

type config struct {
	Port int `koanf:"port"`
}

// Global koanf instance. Use "." as the key path delimiter. This can be "/" or any character.
var k = koanf.New(".")

func main() {
	// Load YAML config.
	if err := k.Load(file.Provider("publisher/config.yml"), yaml.Parser()); err != nil {
		log.Fatalf("loading config: %v", err)
	}
	var cfg config
	k.Unmarshal("config", &cfg)

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
