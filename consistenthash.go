// Package consistenthash provides an implementation of a ring hash.
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
func New(opts ...Option) *ConsistentHash {
	var o options
	for _, opt := range opts {
		opt(&o)
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

	if ch.hash == nil {
		ch.hash = crc32.ChecksumIEEE
	}

	if o.blockPartitioning < 1 {
		o.blockPartitioning = 1
	}

	ch.blockPartitioning = uint32(o.blockPartitioning)
	ch.blockMap = make(map[uint32][]node, ch.blockPartitioning)
	ch.totalBlocks = 1

	return ch
}

// IsEmpty returns true if there are no items available
func (ch *ConsistentHash) IsEmpty() bool {
	return ch.totalKeys == 0
}

// Add adds some keys to the hash
func (ch *ConsistentHash) Add(keys ...[]byte) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.add(ch.replicas, keys...)
}

// AddReplicas adds key and generates "replicas" number of hashes in ring
func (ch *ConsistentHash) AddReplicas(replicas uint, keys ...[]byte) {
	if replicas < 1 {
		return
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.add(replicas, keys...)
}

// Get finds the closest item in the hash ring to the provided key
func (ch *ConsistentHash) Get(key []byte) []byte {
	if ch.IsEmpty() {
		return nil
	}

	hash := ch.hash(key)

	ch.mu.RLock()
	defer ch.mu.RUnlock()

	// check if the exact match exist in the hash table
	if v, ok := ch.hashMap[hash]; ok {
		return v
	}

	v, _ := ch.lookup(hash)
	return v
}

// GetString gets the closest item in the hash ring to the provided key
func (ch *ConsistentHash) GetString(key string) string {
	if v := ch.Get([]byte(key)); v != nil {
		return string(v)
	}
	return ""
}

// Remove removes the key from hash table
func (ch *ConsistentHash) Remove(key []byte) bool {
	if ch.IsEmpty() {
		return true
	}

	originalHash := ch.hash(key)

	ch.mu.Lock()
	defer ch.mu.Unlock()

	replicas, found := ch.replicaMap[originalHash]
	if !found {
		// if not found, means using the default number
		replicas = ch.replicas
	}

	// remove original one
	ch.removeFromBlock(originalHash, originalHash)

	var hash uint32
	var i uint32
	// remove replicas
	for i = 1; i < uint32(replicas); i++ {
		var b bytes.Buffer
		b.Write(key)
		b.Write([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		hash = ch.hash(b.Bytes())
		ch.removeFromBlock(hash, originalHash)
	}
	if found {
		delete(ch.replicaMap, originalHash) // delete replica numbers
	}

	return true
}

// add inserts new hashes in hash table
func (ch *ConsistentHash) add(replicas uint, keys ...[]byte) {
	var hash uint32
	var i uint32
	var h bytes.Buffer
	nodes := make([]node, 0, uint(len(keys))*replicas) // todo avoid overflow
	for idx := range keys {
		originalHash := ch.hash(keys[idx])
		// no need for extra capacity, just get the bytes we need
		ch.hashMap[originalHash] = keys[idx][:len(keys[idx]):len(keys[idx])]
		nodes = append(nodes, node{originalHash, originalHash})
		for i = 1; i < uint32(replicas); i++ {
			h.Write(keys[idx])
			h.WriteByte(byte(i))
			h.WriteByte(byte(i >> 8))
			h.WriteByte(byte(i >> 16))
			h.WriteByte(byte(i >> 24))
			hash = ch.hash(h.Bytes())
			h.Reset()
			nodes = append(nodes, node{hash, originalHash})
		}

		// do not store number of replicas if uses default number
		if replicas != ch.replicas {
			ch.replicaMap[hash] = replicas
		}
	}
	ch.addNodes(nodes)
}

func (ch *ConsistentHash) addNodes(nodes []node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].key < nodes[j].key
	})

	expectedBlocks := (ch.totalKeys + uint32(len(nodes))) / ch.blockPartitioning
	ch.balanceBlocks(expectedBlocks)
	for i := range nodes {
		ch.addNode(nodes[i])
	}
}

func (ch *ConsistentHash) addNode(n node) {
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := n.key / blockSize
	nodes, ok := ch.blockMap[blockNumber]
	if !ok {
		ch.blockMap[blockNumber] = []node{n}
		ch.totalKeys++
		return
	}
	idx := sort.Search(len(nodes), func(i int) bool {
		return nodes[i].key >= n.key
	})

	// check for duplication, ignore if it's duplicate
	if idx < len(nodes) && nodes[idx].key == n.key {
		return
	}

	ch.blockMap[blockNumber] = append(
		ch.blockMap[blockNumber][:idx],
		append([]node{n}, ch.blockMap[blockNumber][idx:]...)...,
	)
	ch.totalKeys++
}

// balanceBlocks checks all the keys in each block and shifts to the next block if the number of blocks needs to be changed
func (ch *ConsistentHash) balanceBlocks(expectedBlocks uint32) {
	// re-balance the blocks if expectedBlocks needs twice size as it's current size
	if (expectedBlocks >> 1) > ch.totalBlocks {
		blockSize := math.MaxUint32 / expectedBlocks
		ch.trigger(EventReBalance, ch.totalBlocks, expectedBlocks, blockSize, ch.totalKeys)
		for blockNumber := uint32(0); blockNumber < expectedBlocks-1; blockNumber++ {
			nodes := ch.blockMap[blockNumber]
			targetBlock := blockNumber
			var j int
			for i := 0; i < len(nodes); i++ {
				// the first item not in the block, so the next items will not be in the block as well
				if nodes[i].key/blockSize == targetBlock {
					continue
				}
				if targetBlock == blockNumber {
					// update current block items
					ch.blockMap[blockNumber] = nodes[:i][:i:i]
				}

				targetBlock++
				for j = i; j < len(nodes); j++ {
					if nodes[j].key/blockSize != targetBlock {
						break
					}
				}
				if i != j {
					ch.trigger(EventReBalanceShift, blockNumber, targetBlock, i, j, len(ch.blockMap[targetBlock]))
					ch.blockMap[targetBlock] = append(nodes[i:j], ch.blockMap[targetBlock]...)
					i = j
				}
			}
		}

		ch.totalBlocks = expectedBlocks
	} else if expectedBlocks < (ch.totalBlocks >> 1) {
		// TODO decrease number of blocks to avoid missed blocks
	}

	if ch.totalBlocks < 1 {
		ch.totalBlocks = 1
	}
}

// removeFromBlock removes one key from a block
func (ch *ConsistentHash) removeFromBlock(hash, originalHash uint32) {
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := hash / blockSize
	_, ok := ch.blockMap[blockNumber]
	if !ok {
		return
	}

	// TODO efficient way would be to use ch.lookup(hash uint32)
	for i := range ch.blockMap[blockNumber] {
		if ch.blockMap[blockNumber][i].key == hash {
			ch.blockMap[blockNumber] = append(ch.blockMap[blockNumber][:i], ch.blockMap[blockNumber][i+1:]...) // remove item
			ch.totalKeys--
			break
		}
	}
	if originalHash == hash {
		delete(ch.hashMap, originalHash)
	}
	return
}

// lookup finds the block number and value of the given hash
func (ch *ConsistentHash) lookup(hash uint32) ([]byte, uint32) {
	// block size is equal to hkeys
	// binary search for appropriate replica
	blockSize := math.MaxUint32 / ch.totalBlocks
	blockNumber := hash / blockSize
	var idx, i int
	var fullCircle bool
	for blockNumber < ch.totalBlocks {
		nodes, ok := ch.blockMap[blockNumber]
		if !ok {
			blockNumber++
			ch.trigger(EventMissedLookupBlock, hash, i)
			i++
			continue
		}
		// binary search inside the block
		idx = sort.Search(len(nodes), func(i int) bool {
			return nodes[i].key >= hash
		})

		// if not found in the block, the first item from the next block is the answer
		if idx == len(nodes) {
			if blockNumber == ch.totalBlocks-1 && !fullCircle {
				// go to the first block
				ch.trigger(EventFullRingLookup, hash, blockNumber)
				blockNumber = 0
				fullCircle = true
			}
			blockNumber++
			continue
		}

		// lookup the pointer in hash table
		return ch.hashMap[nodes[idx].pointer], blockNumber
	}

	// if we reach the last block, we need to find the first block that has an item
	if blockNumber == ch.totalBlocks {
		ch.trigger(EventFullRingLookup, hash, blockNumber)
		var j uint32
		for j < uint32(len(ch.blockMap)) {
			if len(ch.blockMap[j]) > 0 {
				blockNumber = 0
				firstKey := ch.blockMap[0][0].pointer
				return ch.hashMap[firstKey], blockNumber
			}
			ch.trigger(EventMissedLookupBlock, hash, j)
			j++
		}
	}
	return nil, blockNumber
}
