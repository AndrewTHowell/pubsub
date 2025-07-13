package svc

type Message struct {
	Payload []byte
}

type Poller func() ([]Message, error)
