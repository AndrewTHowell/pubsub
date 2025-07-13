package svc

type Message struct {
	Key     string
	Payload []byte
}

type Poller func() ([]Message, error)
