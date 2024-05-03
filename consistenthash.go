// Package distorage provides an implementation of a ring hash.
package distorage

import (
	"bytes"
	"hash/crc32"
	"math"
	"sort"
	"sync"
)

// HashFunc hash function to generate random hash
type HashFunc func(data []byte) uint32

type node struct {
	key     uint32
	pointer uint32
}

// ConsistentHash everything we need for CH
type ConsistentHash struct {
	mu                sync.RWMutex
	hash              HashFunc
	pool              sync.Pool
	replicas          uint              // default number of replicas in hash ring (higher number means more possibility for balance equality)
	hashMap           map[uint32][]byte // Hash table key value pair (hash(x): x) * replicas (nodes)
	replicaMap        map[uint32]uint   // Number of replicas per stored key
	blockMap          map[uint32][]node // fixed size blocks in the circle each might contain a list of keys
	listeners         map[Event][]EventListener
	totalBlocks       uint32
	totalKeys         uint32
	blockPartitioning uint32
}

// New makes new ConsistentHash
func New(opts ...Option) (*ConsistentHash, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.hashFunc == nil {
		return nil, errors.New("hash function is required")
	}
	ch := &ConsistentHash{
		replicas:   o.defaultReplicas,
		hash:       o.hashFunc,
		hashMap:    make(map[uint32][]byte, 0),
		replicaMap: make(map[uint32]uint, 0),
		listeners:  o.listeners,
	}

	if ch.replicas < 1 {
		ch.replicas = 1
	}

	if o.blockPartitioning < 1 {
		o.blockPartitioning = 1
	}

	ch.blockPartitioning = uint32(o.blockPartitioning)
	ch.blockMap = make(map[uint32][]node, ch.blockPartitioning)
	ch.pool = sync.Pool{New: func() interface{} { return make(map[uint32][]node, o.blockPartitioning) }
	ch.totalBlocks = 1

	return ch, nil
}
