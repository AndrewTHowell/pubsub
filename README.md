# PubSub

This is a simple PubSub system with 3 main components:
- Broker
  - Responsible for storing messages and brokering between publishers and subscribers
- Publisher
  - Able to publish messages to topics that Subscribers can listen to via the Broker
- Subscriber
  - Able to subscribe to topics and receive messages from Publishers via the Broker

The components communicate on localhost via gRPC.

## Partitioning

Topics are partitioned, with config determining how many partitions each topic should have. When a
Subscriber spins up, it is assigned partition(s) by the Broker and given a Subscriber ID which it
should use in all future communications.

Messages are partitioned based on hashing a message's key.

Rebalances occur when a new Subscriber needs partitions assigned. Currently a dumb approach is taken
of always re-assigning all partitions to the new Subscriber. This is done for simplicity for now.
Stale Subscribers are not currently removed automatically.

## Roadmap

- Partitioning
  - Different partitioning strategies
  - Sensible new subscriber rebalancing
  - Rebalancing due to stale subscriber
  - What happens if a rebalance occurs whilst a Sub has polled but not moved offset? Chance for
    duplicate processing!
  - Should the Subscriber know what partitions it's assigned, perhaps for observability in case it's
    not assigned any?
- Timestamping messages when received by broker
- Multi-topic publisher
  - Use topic key on message to decide where message should be stored
- Multi-broker support?
