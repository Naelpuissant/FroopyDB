package skiplist

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

var (
	maxHeight = 32
	pValue    = 0.5

	ErrNilKey = errors.New("Nil key not allowed")
)

type Node struct {
	Key    []byte
	Value  []byte
	height int
	levels []*Node
}

func NewNode(Key, Value []byte, height int) *Node {
	return &Node{
		Key:    Key,
		Value:  Value,
		height: height,
		levels: make([]*Node, height+1),
	}
}

func (n *Node) Next() *Node {
	return n.levels[0]
}

type Skiplist struct {
	head   *Node
	last   *Node
	level  int     // current highest level
	update []*Node // Store and reuse update node links
	length atomic.Int64
	size   atomic.Int64
	rand   *rand.Rand
	mu     *sync.RWMutex
}

func New() *Skiplist {
	// head node with max height
	head := NewNode(nil, nil, maxHeight+1)
	update := make([]*Node, maxHeight+1)
	randSrc := rand.NewChaCha8([32]byte{byte(42)})
	return &Skiplist{
		head:   head,
		update: update,
		level:  0,
		rand:   rand.New(randSrc),
		mu:     &sync.RWMutex{},
	}
}

func (l *Skiplist) rHeight() int {
	h := 0
	for h < maxHeight && (l.rand.Float64() < pValue) {
		h++
	}
	return h
}

// Insert adds a key-value pair to the skiplist.
// If the key already exists, it updates the value (O(log(n)))
func (l *Skiplist) Insert(key, value []byte) error {
	if key == nil {
		return ErrNilKey
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	curr := l.head
	for i := maxHeight; i >= 0; i-- {
		for curr.levels[i] != nil && bytes.Compare(curr.levels[i].Key, key) < 0 {
			curr = curr.levels[i]
		}
		l.update[i] = curr
	}
	curr = curr.Next()

	if curr != nil && bytes.Equal(curr.Key, key) {
		l.size.Add(int64(len(value)) - int64(len(curr.Value)))
		curr.Value = value
		return nil
	}

	if curr == nil || !bytes.Equal(curr.Key, key) {
		rHeight := l.rHeight()

		if rHeight > l.level {
			for i := l.level + 1; i <= rHeight; i++ {
				l.update[i] = l.head
			}
			l.level = rHeight
		}

		newNode := NewNode(key, value, rHeight)
		for i := range rHeight + 1 {
			newNode.levels[i] = l.update[i].levels[i]
			l.update[i].levels[i] = newNode
		}

		if newNode.Next() == nil {
			l.last = newNode
		}

		l.size.Add(int64(len(key)) + int64(len(value)))
		l.length.Add(1)
	}
	return nil
}

// Search returns the node with the given key, or nil if not found (O(log(n)))
func (l *Skiplist) Search(key []byte) *Node {
	l.mu.RLock()
	defer l.mu.RUnlock()

	curr := l.head
	for i := l.level; i >= 0; i-- {
		for curr.levels[i] != nil && bytes.Compare(curr.levels[i].Key, key) < 0 {
			curr = curr.levels[i]
		}
	}
	curr = curr.Next()

	if curr != nil && bytes.Equal(curr.Key, key) {
		return curr
	}

	return nil
}

func (l *Skiplist) Range(from []byte, to []byte) []*Node {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if bytes.Compare(from, to) > 0 {
		return nil
	}

	start := l.First()
	if start == nil {
		return nil
	}

	end := l.Last()
	if bytes.Compare(end.Key, from) < 0 || bytes.Compare(start.Key, to) > 0 {
		return nil
	}

	res := []*Node{}

	// find starting point
	curr := l.head
	for i := l.level; i >= 0; i-- {
		for curr.levels[i] != nil && bytes.Compare(curr.levels[i].Key, from) < 0 {
			curr = curr.levels[i]
		}
	}
	curr = curr.Next()

	for ; curr != nil; curr = curr.Next() {
		if bytes.Compare(curr.Key, from) < 0 {
			continue
		}

		if bytes.Compare(curr.Key, to) > 0 {
			break
		}
		res = append(res, curr)
	}

	return res
}

// returns all keys in the skiplist in sorted order (O(n))
func (l *Skiplist) Keys() [][]byte {
	l.mu.RLock()
	defer l.mu.RUnlock()

	res := [][]byte{}
	curr := l.First()
	for curr != nil {
		res = append(res, curr.Key)
		curr = curr.Next()
	}
	return res
}

// Get first element (O(1))
func (l *Skiplist) First() *Node {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.head.Next()
}

// Get last element (O(1))
func (l *Skiplist) Last() *Node {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.last
}

func (l *Skiplist) Length() int64 {
	return l.length.Load()
}

func (l *Skiplist) Size() int64 {
	return l.size.Load()
}
