package svc

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type partitioner interface {
	getPartitionIdx(message Message) int
}

func newPartitioner(partitionStrategy PartitionStrategy, numberOfPartitions int) (partitioner, error) {
	switch partitionStrategy {
	case HashPartition:
		return newHashPartitioner(numberOfPartitions), nil
	case RoundRobinPartition:
		return newRoundRobinPartitioner(numberOfPartitions), nil
	default:
		return nil, fmt.Errorf("unrecognised partition strategy %d", partitionStrategy)
	}
}

type PartitionStrategy int

const (
	HashPartition PartitionStrategy = iota
	RoundRobinPartition
)

type hashPartitioner struct {
	numberOfPartitions int
}

func newHashPartitioner(numberOfPartitions int) *hashPartitioner {
	return &hashPartitioner{
		numberOfPartitions: numberOfPartitions,
	}
}

func (p hashPartitioner) getPartitionIdx(message Message) int {
	hash := fnv.New64a()
	hash.Write([]byte(message.Key))
	return int(hash.Sum64() % uint64(p.numberOfPartitions))
}

type roundRobinPartitioner struct {
	sync.Mutex
	counter, numberOfPartitions int
}

func newRoundRobinPartitioner(numberOfPartitions int) *roundRobinPartitioner {
	return &roundRobinPartitioner{
		numberOfPartitions: numberOfPartitions,
	}
}

func (p *roundRobinPartitioner) getPartitionIdx(message Message) int {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	p.counter = (p.counter + 1) % p.numberOfPartitions
	return p.counter
}
