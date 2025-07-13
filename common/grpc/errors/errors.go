package errors

import (
	"errors"
	"log/slog"
	"os"

	errdetailspb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	commonerrors "pubsub/common/errors"
)

func FromGRPCError(err error) error {
	st := status.Convert(err)

	converter, ok := converterFromGRPCByCode[st.Code()]
	if !ok {
		slog.Error("Unsupported error type", slog.Any("error", err), slog.Any("status_error", st))
		os.Exit(1)
	}
	return converter(st.Message(), st.Details())
}

var converterFromGRPCByCode = map[codes.Code]func(message string, details []any) error{
	codes.FailedPrecondition: func(message string, details []any) error {
		return commonerrors.NewFailedPrecondition(message, preconditionFailuresConverterFromGRPC(details)...)
	},
	codes.InvalidArgument: func(message string, details []any) error {
		return commonerrors.NewInvalidArgument(message, fieldViolationsConverterFromGRPC(details)...)
	},
	codes.Unavailable: func(message string, details []any) error {
		return commonerrors.NewUnavailable(message)
	},
}

func preconditionFailuresConverterFromGRPC(details []any) []commonerrors.PreconditionFailure {
	failures := []commonerrors.PreconditionFailure{}
	if len(details) != 0 {
		preconditionFailureDetail := details[0].(*errdetailspb.PreconditionFailure)
		for _, violation := range preconditionFailureDetail.GetViolations() {
			failures = append(failures, commonerrors.PreconditionFailure{
				Type:        violation.Type,
				Description: violation.Description,
			})
		}
	}
	return failures
}

func fieldViolationsConverterFromGRPC(details []any) []commonerrors.FieldViolation {
	violations := []commonerrors.FieldViolation{}
	if len(details) != 0 {
		badRequestDetail := details[0].(*errdetailspb.BadRequest)
		for _, violation := range badRequestDetail.GetFieldViolations() {
			violations = append(violations, commonerrors.FieldViolation{
				Field:       violation.Field,
				Description: violation.Description,
				Reason:      violation.Reason,
			})
		}
	}
	return violations
}

func ToGRPCError(err error) error {
	failedPrecon := commonerrors.FailedPrecondition{}
	if errors.As(err, &failedPrecon) {
		return toGRPCError(codes.FailedPrecondition, failedPrecon.Message, preconditionFailuresConverterToGRPC(failedPrecon.PreconditionFailures)...)
	}

	invalidArg := commonerrors.InvalidArgument{}
	if errors.As(err, &invalidArg) {
		return toGRPCError(codes.InvalidArgument, invalidArg.Message, fieldViolationsConverterToGRPC(invalidArg.FieldViolations)...)
	}

	unavailable := commonerrors.Unavailable{}
	if errors.As(err, &unavailable) {
		return toGRPCError(codes.Unavailable, unavailable.Message)
	}

	slog.Error("Unsupported error type", slog.Any("error", err))
	os.Exit(1)
	return err
}

func preconditionFailuresConverterToGRPC(violations []commonerrors.PreconditionFailure) []protoadapt.MessageV1 {
	preconditionFailureDetail := &errdetailspb.PreconditionFailure{
		Violations: make([]*errdetailspb.PreconditionFailure_Violation, 0, len(violations)),
	}
	for _, violation := range violations {
		preconditionFailureDetail.Violations = append(preconditionFailureDetail.Violations, &errdetailspb.PreconditionFailure_Violation{
			Type:        violation.Type,
			Description: violation.Description,
		})
	}
	return []protoadapt.MessageV1{preconditionFailureDetail}
}

func fieldViolationsConverterToGRPC(violations []commonerrors.FieldViolation) []protoadapt.MessageV1 {
	badRequestDetail := &errdetailspb.BadRequest{
		FieldViolations: make([]*errdetailspb.BadRequest_FieldViolation, 0, len(violations)),
	}
	for _, violation := range violations {
		badRequestDetail.FieldViolations = append(badRequestDetail.FieldViolations, &errdetailspb.BadRequest_FieldViolation{
			Field:       violation.Field,
			Description: violation.Description,
			Reason:      violation.Reason,
		})
	}
	return []protoadapt.MessageV1{badRequestDetail}
}

func toGRPCError(code codes.Code, message string, details ...protoadapt.MessageV1) error {
	st := status.New(code, message)
	dst, err := st.WithDetails(details...)
	if err != nil {
		slog.Error("Constructing new gRPC status error with details", slog.Any("error", err))
		return st.Err()
	}
	return dst.Err()
}
