package main

import (
	"fmt"
	"log"
	"sync"

	"pubsub/broker"
)

func main() {
	b := broker.New(
		"animals.cats",
		"animals.dogs",
	)

	var pub Publisher = b
	var sub Subscriber = b

	simple(pub, sub)
	//concurrent(pub, sub)
}

func simple(pub Publisher, sub Subscriber) {
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

func concurrent(pub Publisher, sub Subscriber) {
	publishWG := sync.WaitGroup{}

	publish := func(topic string, payload string) {
		pub.Publish(topic, broker.Message{Payload: payload})
		publishWG.Done()
	}
	for i := range 10 {
		publishWG.Add(2)
		go publish("animals.cats", fmt.Sprintf("cat-%d", i))
		go publish("animals.dogs", fmt.Sprintf("dog-%d", i))
	}

	publishWG.Wait()
	pollWG := sync.WaitGroup{}

	poll := func(id int, topic, group string) {
		poll, err := sub.Subscribe(topic, group, 10)
		if err != nil {
			log.Fatalf("Poller %q topic %q group %q subscribing: %v", fmt.Sprint(id), topic, group, err)
		}

		messages, err := poll()
		if err != nil {
			log.Fatalf("Poller %q topic %q group %q polling: %v", fmt.Sprint(id), topic, group, err)
		}
		log.Printf("Poller %q topic %q group %q polled: %+v", fmt.Sprint(id), topic, group, messages)

		pollWG.Done()
	}

	id := 0
	for i := range 3 {
		pollWG.Add(3)
		go poll(id, "animals.cats", fmt.Sprintf("group-%d", i))
		go poll(id+1, "animals.cats", fmt.Sprintf("group-%d", i))
		go poll(id+2, "animals.dogs", fmt.Sprintf("group-%d", i))
		id += 3
	}

	pollWG.Wait()
}

type Publisher interface {
	Publish(topic string, message broker.Message) error
}

type Subscriber interface {
	Subscribe(topic, group string, maxBufferSize int) (broker.Poller, error)
}
