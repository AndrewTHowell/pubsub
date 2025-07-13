package errors

func NewInternal(message string) error {
	return Internal{
		Message: message,
	}
}

type Internal struct {
	Message string
}

func (i Internal) Error() string {
	return i.Message
}
