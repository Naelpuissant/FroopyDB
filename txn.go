package froopydb

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrTxnConflict  = errors.New("Transaction conflict detected")
	ErrTxnCommitted = errors.New("Transaction already committed")
	ErrTxnAborted   = errors.New("Transaction already aborted")
)

type TxnState int

const (
	Active TxnState = iota
	Committed
	Aborted
)

type TxnManager struct {
	commitedTxns []*CommitedTxn
}

func NewTxnManager() *TxnManager {
	return &TxnManager{
		commitedTxns: []*CommitedTxn{},
	}
}

type CommitedTxn struct {
	ts   uint64
	keys map[string]struct{}
}

type Txn struct {
	db     *DB
	state  TxnState
	writes map[string][]byte
	ts     uint64
	mu     sync.Mutex
}

func NewTxn(db *DB) *Txn {
	return &Txn{
		db:     db,
		state:  Active,
		ts:     uint64(time.Now().UnixNano()),
		writes: map[string][]byte{},
	}
}

func (t *Txn) Set(key []byte, value []byte) {
	t.writes[string(key)] = value
}

func (t *Txn) Get(key []byte) []byte {
	if value, ok := t.writes[string(key)]; ok {
		return value
	}
	return t.db.Get(key)
}

// 1 - Set commit ts
// 2 - Check for conflicts
// 3 - Call db add and add keys to current commited txn
func (t *Txn) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state == Committed {
		return ErrTxnCommitted
	}
	if t.state == Aborted {
		return ErrTxnAborted
	}

	// Check for conflicts
	for _, commited := range t.db.TxnManager.commitedTxns {
		if commited.ts > t.ts {
			for key := range t.writes {
				if _, ok := commited.keys[key]; ok {
					t.state = Aborted
					return ErrTxnConflict
				}
			}
		}
	}

	// Later build build a key using commit ts
	commitTs := uint64(time.Now().UnixNano())

	commited := &CommitedTxn{
		ts:   commitTs,
		keys: map[string]struct{}{},
	}
	t.db.TxnManager.commitedTxns = append(t.db.TxnManager.commitedTxns, commited)

	for key, value := range t.writes {
		t.db.Set([]byte(key), value)
		commited.keys[key] = struct{}{}
	}

	t.state = Committed
	return nil
}
