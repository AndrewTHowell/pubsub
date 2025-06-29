package errors

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	errdetailspb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	commonerrors "pubsub/common/errors"
)

type Error struct {
	err     error
	details any
}

func (e Error) Error() string {
	if e.details == nil {
		return e.err.Error()
	}
	return fmt.Sprintf("%s details=%v", e.err, e.details)
}

func FromGRPCError(err error) error {
	st := status.Convert(err)
	switch st.Code() {
	case codes.InvalidArgument:
		violations := []commonerrors.FieldViolation{}
		if len(st.Details()) != 0 {
			badRequestDetail := st.Details()[0].(*errdetailspb.BadRequest)
			for _, violation := range badRequestDetail.GetFieldViolations() {
				violations = append(violations, commonerrors.FieldViolation{
					Field:       violation.Field,
					Description: violation.Description,
					Reason:      violation.Reason,
				})
			}
		}
		return commonerrors.NewInvalidArgument(st.Message(), violations...)
	default:
		slog.Error("Unsupported error type", slog.Any("error", err))
		os.Exit(1)
	}
	return err
}

func ToGRPCError(err error) error {
	invalidArg := commonerrors.InvalidArgument{}
	if errors.As(err, &invalidArg) {
		violations := make([]*errdetailspb.BadRequest_FieldViolation, 0, len(invalidArg.FieldViolations))
		for _, violation := range invalidArg.FieldViolations {
			violations = append(violations, &errdetailspb.BadRequest_FieldViolation{
				Field:       violation.Field,
				Description: violation.Description,
				Reason:      violation.Reason,
			})
		}
		return toGRPCError(codes.InvalidArgument, invalidArg.Message, &errdetailspb.BadRequest{FieldViolations: violations})
	}

	slog.Error("Unsupported error type", slog.Any("error", err))
	os.Exit(1)
	return err
}

func toGRPCError(code codes.Code, message string, details ...protoadapt.MessageV1) error {
	st := status.New(codes.InvalidArgument, message)
	dst, err := st.WithDetails(details...)
	if err != nil {
		slog.Error("Constructing new gRPC status error with details", slog.Any("error", err))
		return st.Err()
	}
	return dst.Err()
}
