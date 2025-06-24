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
		err := FromStatusError(err)
		slog.Error("Publishing", slog.Any("error", err))
		os.Exit(1)
	}
}

type Error struct {
	err     error
	details any
}

func (e Error) Error() string {
	return fmt.Sprintf("%s details=%v", e.err, e.details)
}

func FromStatusError(err error) error {
	st := status.Convert(err)
	var errDetails any
	for _, d := range st.Details() {
		switch info := d.(type) {
		case *errdetails.BadRequest:
			errDetails = info.GetFieldViolations()
		default:
			slog.Error("Unsupported error details type", slog.Any("details", info))
		}
	}
	return Error{
		err:     st.Err(),
		details: errDetails,
	}
}

func toPtr[P *T, T any](t T) P { return &t }
