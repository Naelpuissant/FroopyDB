package table

import "iter"

type Base interface {
	KVIter() iter.Seq2[string, []byte]
	MinKey() []byte
}
