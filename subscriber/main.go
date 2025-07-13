package main

import (
	"context"
	"fmt"
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
	slog.Debug("Config loaded", slog.Any("config", cfg))

	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(grpcerrors.UnaryClientInterceptor),
	)
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", cfg.Port), opts...)
	if err != nil {
		slog.Error("Dialling", slog.Any("error", err))
		os.Exit(1)
	}
	defer conn.Close()

	client := brokerpb.NewBrokerClient(conn)

	resp, err := client.Subscribe(context.Background(), brokerpb.SubscribeRequest_builder{
		Topic: toPtr("animals.cats"),
		Group: toPtr("animals.cats.group"),
	}.Build())
	if err != nil {
		slog.Error("Subscribing", slog.Any("error", err))
		os.Exit(1)
	}
	subscriberID := resp.GetSubscriberId()

	for {
		resp, err := client.Poll(context.Background(), brokerpb.PollRequest_builder{
			SubscriberId: &subscriberID,
			Limit:        toPtr(int32(10)),
		}.Build())
		if err != nil {
			slog.Error("Polling", slog.Any("error", err))
			os.Exit(1)
		}

		for _, message := range resp.GetMessages() {
			fmt.Println(string(message.GetPayload()))
		}

		if len(resp.GetMessages()) == 0 {
			continue
		}

		request := brokerpb.MoveOffsetRequest_builder{
			SubscriberId: &subscriberID,
			Delta:        toPtr(int32(len(resp.GetMessages()))),
		}.Build()
		if _, err := client.MoveOffset(context.Background(), request); err != nil {
			slog.Error("Moving offset", slog.Any("error", err))
			os.Exit(1)
		}
	}
}

func toPtr[P *T, T any](t T) P { return &t }
