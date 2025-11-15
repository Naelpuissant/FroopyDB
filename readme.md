# FroopyDB

A persistent LSM-tree based/key-value db for educational purpose


## Goal

* Learn more about go
* Learn more about db architectures
* Start simple and improve perf after


## File formats

SSTable
```
Data
vlen uint16 | value bytes | ... | vlen uint16 | value bytes
Metadata
klen uint16 | key bytes | offset uint64 | ... | indicesStartOffset uint16
```

WAL
```
klen uint16 | vlen uint 16 | key bytes | value bytes 
```

## Bench

pure file (seq read) bench
```
goos: linux
goarch: amd64
pkg: froopydb/src
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkSet-8            157388              7812 ns/op
BenchmarkGet-8             10000           2001152 ns/op
PASS
ok      froopydb/src    22.340s
```


LSM tree based, skiplist memtable (1000B), wal, no compaction/no bloom filters/no parallel jobs (100 000 ops)
```
goos: linux
goarch: amd64
pkg: froopydb/src
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkSet
BenchmarkSet-8            100000             33296 ns/op
BenchmarkGet
BenchmarkGet-8            100000             16241 ns/op
PASS
ok      froopydb/src    5.027s
```
Since everything is done on a single thread we might have some heavy spikes on set when memtable is flushed


## TODO

- [x] MemTable/log (skiplist)
- [x] Work with bytes arrays (keep keys as int in db call)
- [x] create file format for segments (rename SSTable)
- [x] Log bin format 
- [x] crash or start recovery
- [ ] compaction process
- [ ] parallel processing
- [ ] clean error handling (too add when bored)
- [ ] refactor method privacy (too add when bored)
- [ ] range queries
- [ ] Bloom filter
- [ ] Skiplist custom
- [ ] MMap potential use and benefits


## Usefull links
- https://github.com/Naelpuissant/mehdb/blob/main/document.pdf
- https://www.nan.fyi/database
- https://github.com/dgraph-io/badger/tree/main
- https://github.com/rosedblabs/wal/tree/main