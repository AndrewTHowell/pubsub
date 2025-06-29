package errors

func NewUnavailable(message string) error {
	return Unavailable{
		Message: message,
	}
}

type Unavailable struct {
	Message string
}

func (i Unavailable) Error() string {
	return i.Message
}
