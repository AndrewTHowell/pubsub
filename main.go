package main

import (
	"log"
	"time"

	"pubsub/broker"
)

func main() {
	b := broker.New(
		10,
		"animals.cats",
		"animals.dogs",
	)

	var pub Publisher = b
	var sub Subscriber = b

	catsChan, err := sub.Subscribe("animals.cats")
	if err != nil {
		log.Fatalf("Subscribing to 'animals.cats' topic: %v", err)
	}

	pub.Publish("animals.cats", broker.Message{Payload: "walter"})

	select {
	case c := <-catsChan:
		log.Printf("Received cat: %v\n", c)
	case <-time.After(5 * time.Second):
		log.Fatalf("Timed out waiting for cat")
	}
}

type Publisher interface {
	Publish(topic string, message broker.Message) error
}

type Subscriber interface {
	Subscribe(topic string) (<-chan broker.Message, error)
}
