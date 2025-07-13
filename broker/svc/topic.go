package svc

import (
	"fmt"
	"hash/fnv"
	commonerrors "pubsub/common/errors"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

type TopicDefinition struct {
	Name               string
	NumberOfPartitions int
}

type topic struct {
	mutex sync.RWMutex

	name               string
	numberOfPartitions int

	partitioner     func(Message) int
	subscribersByID map[string]subscriber

	partitionedMessages      []*[]Message
	offsetByPartitionByGroup map[string]map[int]int
}

type subscriber struct {
	group      string
	partitions []int
}

func newTopic(name string, numberOfPartitions int) (*topic, error) {
	if numberOfPartitions < 1 {
		return nil, fmt.Errorf("invalid number of partitions: must be greater than zero, got %d", numberOfPartitions)
	}

	hashPartitioner := func(m Message) int {
		hash := fnv.New64a()
		hash.Write([]byte(m.Key))
		return int(hash.Sum64() % uint64(numberOfPartitions))
	}
	partitionedMessages := make([]*[]Message, 0, numberOfPartitions)
	for range numberOfPartitions {
		partitionedMessages = append(partitionedMessages, &[]Message{})
	}
	return &topic{
		name:                     name,
		numberOfPartitions:       numberOfPartitions,
		partitioner:              hashPartitioner,
		partitionedMessages:      partitionedMessages,
		offsetByPartitionByGroup: map[string]map[int]int{},
	}, nil
}

func (t *topic) publish(newMessages ...Message) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	now := time.Now().UTC()
	for _, message := range newMessages {
		message.Timestamp = now
		messages := t.partitionedMessages[t.partitioner(message)]
		*messages = append(*messages, message)
	}
	return nil
}

func (t *topic) subscribe(group string) string {
	subscriberID := uuid.NewV4().String()

	// Naive (dumb) assignment of partitions: reassign all to new subscriber.
	partitions := make([]int, 0, t.numberOfPartitions)
	for i := range t.numberOfPartitions {
		partitions = append(partitions, i)
	}
	t.subscribersByID = map[string]subscriber{
		subscriberID: {
			group:      group,
			partitions: partitions,
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

	offsetByPartition := t.offsetByPartitionByGroup[subscriber.group]
	for _, partition := range subscriber.partitions {
		offset := offsetByPartition[partition]
		if offset == len(*t.partitionedMessages[partition]) {
			// Group has polled all messages in this partition.
			continue
		}

		end := min(offset+(maxBufferSize-len(polledMessages)), len(*t.partitionedMessages[partition]))

		messages := *t.partitionedMessages[partition]
		polledMessages = append(polledMessages, messages[offset:end]...)

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

	if _, ok := t.offsetByPartitionByGroup[subscriber.group]; !ok {
		t.offsetByPartitionByGroup[subscriber.group] = map[int]int{}
	}
	offsetByPartition := t.offsetByPartitionByGroup[subscriber.group]
	for _, partition := range subscriber.partitions {
		newOffset := min(offsetByPartition[partition]+remainingDelta, len(*t.partitionedMessages[partition]))
		partitionDelta := newOffset - offsetByPartition[partition]

		if _, ok := offsetByPartition[partition]; !ok {
			offsetByPartition[partition] = 0
		}
		offsetByPartition[partition] += partitionDelta
		remainingDelta -= partitionDelta

		if remainingDelta == 0 {
			break
		}
	}

	if remainingDelta > 0 {
		return fmt.Errorf("moving offset: %w", errInvalidOffsetDelta{delta: delta})
	}
	return nil
}
