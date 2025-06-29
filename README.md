# PubSub

This is a simple PubSub system with 3 main components:
- Broker
  - Responsible for storing messages and brokering between publishers and subscribers
- Publisher
  - Able to publish messages to topics that Subscribers can listen to via the Broker
- Subscriber
  - Able to subscribe to topics and receive messages from Publishers via the Broker

The components communicate on localhost via gRPC.
