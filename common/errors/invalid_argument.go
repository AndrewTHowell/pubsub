package errors

import (
	"fmt"
	"strings"
)

func NewInvalidArgument(message string, violations ...FieldViolation) error {
	return InvalidArgument{
		Message:         message,
		FieldViolations: violations,
	}
}

type InvalidArgument struct {
	Message         string
	FieldViolations FieldViolations
}

func (i InvalidArgument) Error() string {
	return i.Message + ", violations=" + i.FieldViolations.String()
}

type FieldViolations []FieldViolation

func (v FieldViolations) String() string {
	sep := ", "
	str := "["
	for _, violation := range v {
		str += violation.String() + sep
	}
	// Remove trailing separator if it's there, i.e. if len(v) != 0.
	str = strings.TrimSuffix(str, sep)
	str += "]"
	return str
}

type FieldViolation struct {
	Field,
	Description,
	Reason string
}

func (v FieldViolation) String() string {
	return fmt.Sprintf("field=%q desc=%q reason=%q", v.Field, v.Description, v.Reason)
}
