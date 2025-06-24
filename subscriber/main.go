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
	cfg, err := config.ParseYAML[Config]("subscriber/config.yml", "config")
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

	for {
		messages, err := client.Poll(context.Background(), brokerpb.PollRequest_builder{
			Topic: toPtr("animals.cats"),
			Group: toPtr("animals.cats.group"),
			Limit: toPtr(int32(10)),
		}.Build())
		if err != nil {
			log.Fatalf("polling: %v", err)
		}

		for i, message := range messages.GetMessages() {
			log.Printf("%d: %+s\n", i, string(message.GetPayload()))
		}

		request := brokerpb.MoveOffsetRequest_builder{
			Topic: toPtr("animals.cats"),
			Group: toPtr("animals.cats.group"),
			Delta: toPtr(int32(len(messages.GetMessages()))),
		}.Build()
		if _, err := client.MoveOffset(context.Background(), request); err != nil {
			log.Fatalf("polling: %v", err)
		}
	}
}

func toPtr[P *T, T any](t T) P { return &t }
