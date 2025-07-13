package svc

import (
	"sync"
)

type partition struct {
	mutex sync.RWMutex

	messages      *[]Message
	offsetByGroup map[string]int
}

func newPartition() *partition {
	return &partition{
		messages:      &[]Message{},
		offsetByGroup: map[string]int{},
	}
}

func (p *partition) publish(newMessages ...Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	*p.messages = append(*p.messages, newMessages...)
}

func (p *partition) poll(group string, limit int) []Message {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	offset := p.offsetByGroup[group]
	if offset == len(*p.messages) {
		// Group has polled all messages in this partition.
		return nil
	}

	end := min(offset+limit, len(*p.messages))
	polledMessages := *p.messages
	return polledMessages[offset:end]
}

// Move the offset by the given delta. The given delta can exceed the current partition, so the
// remainder is returned.
func (p *partition) moveOffset(group string, delta int) int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	offset := p.offsetByGroup[group]
	newOffset := min(offset+delta, len(*p.messages))
	p.offsetByGroup[group] = newOffset

	return offset + delta - newOffset
}
