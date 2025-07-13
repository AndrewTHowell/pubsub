package svc

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type TopicDefinition struct {
	Name               string
	NumberOfPartitions int
}

type Topic struct {
	mutex sync.RWMutex

	partitioner         func(Message) int
	partitionedMessages []*[]Message
	offsetByGroup       map[string]int
}

func newTopic(numberOfPartitions int) (*Topic, error) {
	if numberOfPartitions < 1 {
		return nil, fmt.Errorf("invalid number of partitions: must be greater than zero, got %d", numberOfPartitions)
	}

	hashPartitioner := func(m Message) int {
		hash := fnv.New64a()
		hash.Write([]byte(m.Key))
		return int(hash.Sum64() % uint64(numberOfPartitions))
	}

	return &Topic{
		partitioner:         hashPartitioner,
		partitionedMessages: make([]*[]Message, numberOfPartitions),
		offsetByGroup:       map[string]int{},
	}, nil
}

func (t *Topic) publish(newMessages ...Message) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for _, message := range newMessages {
		messages := t.partitionedMessages[t.partitioner(message)]
		*messages = append(*messages, message)
	}
	return nil
}

func (t *Topic) subscribe(group string, maxBufferSize int) Poller {
	return func() ([]Message, error) {
		messages, err := t.poll(group, maxBufferSize)
		if err != nil {
			return nil, err
		}

		if err := t.moveOffset(group, len(messages)); err != nil {
			return nil, err
		}

		return messages, nil
	}
}

func (t *Topic) poll(group string, maxBufferSize int) ([]Message, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	offset := t.offsetByGroup[group]
	if offset == len(*t.messages) {
		// Group has polled all messages.
		return nil, nil
	}

	end := min(offset+maxBufferSize, len(*t.messages))

	polledMessages := *t.messages
	polledMessages = polledMessages[offset:end]

	return polledMessages, nil
}

func (t *Topic) moveOffset(group string, delta int) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	newOffset := t.offsetByGroup[group] + delta
	if newOffset > len(*t.messages) {
		return fmt.Errorf("committing: %w", errInvalidOffsetDelta{delta: delta})
	}
	t.offsetByGroup[group] = newOffset

	return nil
}
