package table

import err "errors"

var (
	ErrSSTableIndexRecoveryFailed = err.New("failed to recover sstable index")
)
