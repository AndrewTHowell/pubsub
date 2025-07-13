package svc

import (
	"fmt"

	commonerrors "pubsub/common/errors"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

type TopicDefinition struct {
	Name               string
	NumberOfPartitions int
	PartitionStrategy  PartitionStrategy
}

type topic struct {
	mutex sync.RWMutex

	name            string
	partitions      []*partition
	partitioner     partitioner
	subscribersByID map[string]subscriber
}

type subscriber struct {
	group         string
	partitionIdxs []int
}

func newTopic(name string, numberOfPartitions int, partitionStrategy PartitionStrategy) (*topic, error) {
	if numberOfPartitions < 1 {
		return nil, fmt.Errorf("creating topic %q: number of partitions must be greater than zero, got %d", name, numberOfPartitions)
	}

	partitioner, err := newPartitioner(partitionStrategy, numberOfPartitions)
	if err != nil {
		return nil, fmt.Errorf("creating topic %q: %w", name, err)
	}
	partitions := make([]*partition, 0, numberOfPartitions)
	for range numberOfPartitions {
		partitions = append(partitions, newPartition())
	}
	return &topic{
		name:        name,
		partitions:  partitions,
		partitioner: partitioner,
	}, nil
}

func (t *topic) publish(newMessages ...Message) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	now := time.Now().UTC()
	for _, message := range newMessages {
		message.Timestamp = now
		partition := t.partitions[t.partitioner.getPartitionIdx(message)]
		partition.publish(message)
	}
	return nil
}

func (t *topic) subscribe(group string) string {
	subscriberID := uuid.NewV4().String()

	// Naive (dumb) assignment of partitions: reassign all to new subscriber.
	partitions := make([]int, 0, len(t.partitions))
	for i := range len(t.partitions) {
		partitions = append(partitions, i)
	}
	t.subscribersByID = map[string]subscriber{
		subscriberID: {
			group:         group,
			partitionIdxs: partitions,
		},
	}
	return subscriberID
}

func (t *topic) poll(subscriberID string, maxBufferSize int) ([]Message, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	subscriber, ok := t.subscribersByID[subscriberID]
	if !ok {
		return nil, commonerrors.NewFailedPrecondition("invalid polling request", commonerrors.PreconditionFailure{
			Type:        errSubscriberNotFound,
			Description: fmt.Sprintf("Subscriber %q is not a registered subscriber of the broker.", subscriberID),
		})
	}

	polledMessages := make([]Message, 0, maxBufferSize)
	limit := maxBufferSize
	for _, partitionIdx := range subscriber.partitionIdxs {
		partition := t.partitions[partitionIdx]
		messages := partition.poll(subscriber.group, limit)

		polledMessages = append(polledMessages, messages...)
		limit -= len(messages)

		if len(polledMessages) == maxBufferSize {
			break
		}
	}
	return polledMessages, nil
}

func (t *topic) moveOffset(subscriberID string, delta int) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	subscriber, ok := t.subscribersByID[subscriberID]
	if !ok {
		return commonerrors.NewFailedPrecondition("invalid polling request", commonerrors.PreconditionFailure{
			Type:        errSubscriberNotFound,
			Description: fmt.Sprintf("Subscriber %q is not a registered subscriber of the broker.", subscriberID),
		})
	}

	remainingDelta := delta

	for _, partitionIdx := range subscriber.partitionIdxs {
		partition := t.partitions[partitionIdx]

		remainingDelta = partition.moveOffset(subscriber.group, remainingDelta)
		if remainingDelta == 0 {
			break
		}
	}

	if remainingDelta > 0 {
		return fmt.Errorf("moving offset: %w", errInvalidOffsetDelta{delta: delta})
	}
	return nil
}
