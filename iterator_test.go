package froopydb

import (
	"iter"
	"reflect"
	"testing"

	"froopydb/x"
)

type iterKV struct {
	key string
	ts  uint64
	val string
}

func seqFromKVs(kvs ...iterKV) iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for _, kv := range kvs {
			if !yield(string(x.EncodeKey([]byte(kv.key), kv.ts)), []byte(kv.val)) {
				return
			}
		}
	}
}

func iteratorTables(seqs ...iter.Seq2[string, []byte]) iter.Seq[iter.Seq2[string, []byte]] {
	return func(yield func(iter.Seq2[string, []byte]) bool) {
		for _, seq := range seqs {
			if !yield(seq) {
				return
			}
		}
	}
}

func collectIterator(t *testing.T, it *Iterator) [][2]string {
	t.Helper()
	defer it.Close()

	got := [][2]string{}
	for it.Start(); it.Ok(); it.Next() {
		k, _ := x.DecodeKey(it.Key)
		got = append(got, [2]string{string(k), string(it.Value)})
	}
	return got
}

// sst | | | |3|4|5|6| |
// sst |0|1|2| |4| | | |
// imm | |1|2|3| | | | |
// mem |0| |2|3| | | |7|
func TestIteratorReadmeCursorMergeUseCase(t *testing.T) {

	mem := seqFromKVs(
		iterKV{"000", 4, "mem-000"},
		iterKV{"002", 4, "mem-002"},
		iterKV{"003", 4, "mem-003"},
		iterKV{"007", 4, "mem-007"},
	)
	imm := seqFromKVs(
		iterKV{"001", 3, "imm-001"},
		iterKV{"002", 3, "imm-002"},
		iterKV{"003", 3, "imm-003"},
	)
	sst2 := seqFromKVs(
		iterKV{"000", 2, "sst2-000"},
		iterKV{"001", 2, "sst2-001"},
		iterKV{"002", 2, "sst2-002"},
		iterKV{"004", 2, "sst2-004"},
	)
	sst1 := seqFromKVs(
		iterKV{"003", 1, "sst1-003"},
		iterKV{"004", 1, "sst1-004"},
		iterKV{"005", 1, "sst1-005"},
		iterKV{"006", 1, "sst1-006"},
	)

	it, err := NewIterator(iteratorTables(mem, imm, sst2, sst1), 10)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	got := collectIterator(t, it)
	expected := [][2]string{
		{"000", "mem-000"},
		{"001", "imm-001"},
		{"002", "mem-002"},
		{"003", "mem-003"},
		{"004", "sst2-004"},
		{"005", "sst1-005"},
		{"006", "sst1-006"},
		{"007", "mem-007"},
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected iterator output\n got: %#v\nwant: %#v", got, expected)
	}
}

func TestIteratorEmptyTables(t *testing.T) {
	it, err := NewIterator(iteratorTables(
		seqFromKVs(),
		seqFromKVs(),
	), 10)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	got := collectIterator(t, it)
	if len(got) != 0 {
		t.Fatalf("empty iterator should yield nothing, got: %#v", got)
	}
}

func TestIteratorSingleCursorYieldsLastEntry(t *testing.T) {
	it, err := NewIterator(iteratorTables(seqFromKVs(
		iterKV{"001", 1, "one"},
		iterKV{"002", 1, "two"},
		iterKV{"003", 1, "three"},
	)), 10)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	got := collectIterator(t, it)
	expected := [][2]string{
		{"001", "one"},
		{"002", "two"},
		{"003", "three"},
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected iterator output\n got: %#v\nwant: %#v", got, expected)
	}
}

func TestIteratorRespectsReadTimestamp(t *testing.T) {
	it, err := NewIterator(iteratorTables(
		seqFromKVs(
			iterKV{"001", 5, "future-001"},
			iterKV{"002", 5, "future-002"},
		),
		seqFromKVs(
			iterKV{"001", 2, "visible-001"},
			iterKV{"002", 2, "visible-002"},
			iterKV{"003", 2, "visible-003"},
		),
	), 3)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	got := collectIterator(t, it)
	expected := [][2]string{
		{"001", "visible-001"},
		{"002", "visible-002"},
		{"003", "visible-003"},
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected iterator output\n got: %#v\nwant: %#v", got, expected)
	}
}

func TestIteratorFutureTombstoneDoesNotHideVisibleValue(t *testing.T) {
	it, err := NewIterator(iteratorTables(
		seqFromKVs(iterKV{"001", 5, string([]byte{0x00})}),
		seqFromKVs(iterKV{"001", 2, "visible-001"}),
	), 3)

	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	got := collectIterator(t, it)
	expected := [][2]string{{"001", "visible-001"}}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected iterator output\n got: %#v\nwant: %#v", got, expected)
	}
}

func TestIteratorSkipsKeyWithOnlyTombstones(t *testing.T) {
	it, err := NewIterator(iteratorTables(
		seqFromKVs(iterKV{"001", 5, string([]byte{0x00})}),
		seqFromKVs(iterKV{"001", 2, string([]byte{0x00})}),
		seqFromKVs(iterKV{"002", 1, "two"}),
	), 10)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	got := collectIterator(t, it)
	expected := [][2]string{{"002", "two"}}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected iterator output\n got: %#v\nwant: %#v", got, expected)
	}
}

func TestIteratorSkipsLatestVisibleTombstone(t *testing.T) {
	mem := seqFromKVs(
		iterKV{"002", 5, string([]byte{0x00})},
		iterKV{"003", 5, "mem-003"},
	)
	imm := seqFromKVs(
		iterKV{"002", 4, "imm-002"},
	)
	sst := seqFromKVs(
		iterKV{"001", 1, "sst-001"},
		iterKV{"002", 1, "sst-002"},
		iterKV{"004", 1, "sst-004"},
	)

	it, err := NewIterator(iteratorTables(mem, imm, sst), 10)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	got := collectIterator(t, it)
	expected := [][2]string{
		{"001", "sst-001"},
		{"003", "mem-003"},
		{"004", "sst-004"},
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected iterator output\n got: %#v\nwant: %#v", got, expected)
	}
}
