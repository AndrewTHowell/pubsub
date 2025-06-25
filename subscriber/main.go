package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	brokerpb "pubsub/broker/proto/broker"
	"pubsub/common/config"
	grpcerrors "pubsub/common/grpc/errors"
)

type Config struct {
	Port int `koanf:"port"`
}

func main() {
	cfg, err := config.ParseYAML[Config]("subscriber/config.yml", "config")
	if err != nil {
		slog.Error("Parsing config", slog.Any("error", err))
		os.Exit(1)
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", cfg.Port), opts...)
	if err != nil {
		slog.Error("Dialling", slog.Any("error", err))
		os.Exit(1)
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
			slog.Error("Polling", slog.Any("error", err))
			os.Exit(1)
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
			err := grpcerrors.FromStatusError(err)
			slog.Error("Moving offset", slog.Any("error", err))
			os.Exit(1)
		}
	}
}

func toPtr[P *T, T any](t T) P { return &t }
