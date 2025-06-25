package errors

import (
	"fmt"
	"log/slog"

	errdetailspb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Error struct {
	err     error
	details any
}

func (e Error) Error() string {
	if e.details == nil {
		return e.Error()
	}
	return fmt.Sprintf("%s details=%v", e.err, e.details)
}

func FromStatusError(err error) error {
	st := status.Convert(err)
	var errDetails any
	for _, d := range st.Details() {
		switch info := d.(type) {
		case *errdetailspb.BadRequest:
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

func NewInvalidArgument(msg string, violations ...*errdetailspb.BadRequest_FieldViolation) error {
	st := status.New(codes.InvalidArgument, msg)
	dst, err := st.WithDetails(&errdetailspb.BadRequest{FieldViolations: violations})
	if err != nil {
		slog.Error("Constructing new gRPC status error with details", slog.Any("error", err))
		return st.Err()
	}
	return dst.Err()
}
