package froopydb

import (
	"bytes"
	"froopydb/x"
	"iter"
)

type Cursor struct {
	next func() (string, []byte, bool)
	stop func()
	key  string
	val  []byte
	ok   bool
}

// PlainKey returns the key without the timestamp suffix
func (c *Cursor) PlainKey() []byte {
	plainKey, _ := x.DecodeKey([]byte(c.key))
	return plainKey
}

// Ts returns the timestamp suffix of the key
func (c *Cursor) Ts() uint64 {
	_, ts := x.DecodeKey([]byte(c.key))
	return ts
}

type Iterator struct {
	cursors []*Cursor
	ts      int
	curr    *Cursor
	Key     []byte
	Value   []byte
	ok      bool
}

func NewIterator(tables iter.Seq[iter.Seq2[string, []byte]], ts int) (*Iterator, error) {
	cursors := []*Cursor{}
	for table := range tables {
		next, stop := iter.Pull2(table)
		key, val, ok := next()
		cursors = append(cursors, &Cursor{
			next: next,
			stop: stop,
			key:  key,
			val:  val,
			ok:   ok,
		})
	}

	return &Iterator{
		cursors: cursors,
		ts:      ts,
	}, nil
}

func (it *Iterator) Ok() bool {
	return it.ok
}

func (it *Iterator) Start() ([]byte, []byte) {
	it.Next()
	return it.Key, it.Value
}

func (it *Iterator) setMinCursor() {
	it.curr = nil

	for _, c := range it.cursors {
		if !c.ok {
			continue
		}

		if it.curr == nil || c.key < it.curr.key {
			it.curr = c
		}
	}
}

// emitOrSkip emits the current key-value pair if it is not marked as deleted (tombstone).
// returns true if the key-value pair was emitted, false if it was skipped.
func (it *Iterator) emitOrSkip(key []byte, val []byte) bool {
	// Skip if deleted or nil
	if key == nil || (len(val) > 0 && val[0] == 0x00) {
		return false
	}

	it.Key = key
	it.Value = val
	it.ok = true
	return true
}

func (it *Iterator) getMinCursor() *Cursor {
	var minCursor *Cursor
	for _, c := range it.cursors {
		if !c.ok {
			continue
		}
		if minCursor == nil || c.key < minCursor.key {
			minCursor = c
		}
	}
	return minCursor
}

func (it *Iterator) Next() {
	var candidate [2][]byte
	for {
		minCursor := it.getMinCursor()
		if minCursor == nil { // we hit the end, purge
			purged := it.emitOrSkip(candidate[0], candidate[1])
			it.ok = purged
			return
		}

		current := [2][]byte{[]byte(minCursor.key), minCursor.val}

		if len(candidate[0]) == 0 {
			candidate = current
			minCursor.key, minCursor.val, minCursor.ok = minCursor.next()
			continue
		}

		candidateKey, candidateTs := x.DecodeKey(candidate[0])
		currentKey, currentTs := x.DecodeKey(current[0])

		if bytes.Equal(candidateKey, currentKey) {
			// skip if current ts is above txn ts
			// we could also skip until next key since all keys above should be skiped
			if currentTs <= uint64(it.ts) && currentTs > candidateTs {
				candidate = current
			}
			minCursor.key, minCursor.val, minCursor.ok = minCursor.next()
			continue
		}

		// reset candidate if it has been deleted
		if !it.emitOrSkip(candidate[0], candidate[1]) {
			candidate = [2][]byte{}
			continue
		}

		return
	}
}

func (it *Iterator) Close() {
	for _, cursor := range it.cursors {
		cursor.stop()
	}
}
