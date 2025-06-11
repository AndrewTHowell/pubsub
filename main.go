package main

import (
	"log"

	"pubsub/broker"
)

func main() {
	b := broker.New(
		"animals.cats",
		"animals.dogs",
	)

	var pub Publisher = b
	var sub Subscriber = b

	poll, err := sub.Subscribe("animals.cats", "group-1", 10)
	if err != nil {
		log.Fatalf("Subscribing to 'animals.cats' topic: %v", err)
	}

	pub.Publish("animals.cats", broker.Message{Payload: "walter"})

	messages, err := poll()
	if err != nil {
		log.Fatalf("Polling 'animals.cats' topic: %v", err)
	}

	log.Printf("Polled messages: %+v", messages)

	messages, err = poll()
	if err != nil {
		log.Fatalf("Polling 'animals.cats' topic: %v", err)
	}

	log.Printf("Polled messages: %+v", messages)
}

type Publisher interface {
	Publish(topic string, message broker.Message) error
}

type Subscriber interface {
	Subscribe(topic, group string, maxBufferSize int) (broker.Poller, error)
}
