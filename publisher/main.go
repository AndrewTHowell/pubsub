package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	brokerpb "pubsub/broker/proto/broker"
	"pubsub/common/config"
)

type Config struct {
	Port int `koanf:"port"`
}

func main() {
	cfg, err := config.ParseYAML[Config]("publisher/config.yml", "config")
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

	request := brokerpb.PublishRequest_builder{
		Messages: []*brokerpb.Message{brokerpb.Message_builder{}.Build()},
	}.Build()
	if _, err := client.Publish(context.Background(), request); err != nil {
		st := status.Convert(err)
		var errDetails any
		for _, d := range st.Details() {
			switch info := d.(type) {
			case *errdetails.BadRequest:
				errDetails = info.GetFieldViolations()
			default:
				slog.Error("Unexpected error details", slog.Any("details", info))
			}
		}
		slog.Error("Publishing", slog.Any("error", err), slog.Any("details", errDetails))
		os.Exit(1)
	}
}

func toPtr[P *T, T any](t T) P { return &t }
