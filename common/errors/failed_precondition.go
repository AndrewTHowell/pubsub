package errors

import (
	"fmt"
	"strings"
)

func NewFailedPrecondition(message string, preconditionFailures ...PreconditionFailure) error {
	return FailedPrecondition{
		Message:              message,
		PreconditionFailures: preconditionFailures,
	}
}

type FailedPrecondition struct {
	Message              string
	PreconditionFailures PreconditionFailures
}

func (i FailedPrecondition) Error() string {
	return i.Message + ", precondition_failures=" + i.PreconditionFailures.String()
}

type PreconditionFailures []PreconditionFailure

func (f PreconditionFailures) String() string {
	sep := ", "
	str := "["
	for _, failure := range f {
		str += failure.String() + sep
	}
	// Remove trailing separator if it's there, i.e. if len(f) != 0.
	str = strings.TrimSuffix(str, sep)
	str += "]"
	return str
}

type PreconditionFailure struct {
	Type,
	Description string
}

func (f PreconditionFailure) String() string {
	return fmt.Sprintf("type=%q desc=%q", f.Type, f.Description)
}
