package froopydb

import (
	"errors"
	"sync"
	"time"

	"froopydb/x"
)

var (
	ErrTxnConflict  = errors.New("transaction conflict detected")
	ErrTxnCommitted = errors.New("transaction already committed")
	ErrTxnAborted   = errors.New("transaction already aborted")
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

func (t *Txn) checkActive() error {
	if t.state == Committed {
		return ErrTxnCommitted
	}
	if t.state == Aborted {
		return ErrTxnAborted
	}
	return nil
}

func (t *Txn) Delete(key []byte) {
	if err := t.checkActive(); err != nil {
		panic(err)
	}
	t.writes[string(key)] = []byte{0x00} // Tombstone value
}

func (t *Txn) Set(key []byte, value []byte) {
	if err := t.checkActive(); err != nil {
		panic(err)
	}
	t.writes[string(key)] = value
}

func (t *Txn) Get(key []byte) []byte {
	if err := t.checkActive(); err != nil {
		panic(err)
	}
	if value, ok := t.writes[string(key)]; ok {
		if len(value) == 1 && value[0] == 0x00 {
			return nil
		}
		return value
	}
	return t.db.Get(x.EncodeKey(key, t.ts))
}

// Commit the transaction
// 1 - Set commit ts
// 2 - Check for conflicts
// 3 - Call db add and add keys to current commited txn
func (t *Txn) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.checkActive(); err != nil {
		return err
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
	commitTS := uint64(time.Now().UnixNano())

	commited := &CommitedTxn{
		ts:   commitTS,
		keys: map[string]struct{}{},
	}
	t.db.TxnManager.commitedTxns = append(t.db.TxnManager.commitedTxns, commited)

	for key, value := range t.writes {
		encodedKey := x.EncodeKey([]byte(key), commitTS)
		t.db.Set(encodedKey, value)
		commited.keys[key] = struct{}{}
	}

	t.state = Committed
	return nil
}
