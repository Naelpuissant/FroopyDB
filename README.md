[![Go](https://github.com/Naelpuissant/FroopyDB/actions/workflows/go.yml/badge.svg)](https://github.com/Naelpuissant/FroopyDB/actions/workflows/go.yml)
![Coverage](https://img.shields.io/badge/Coverage-61.3%25-yellow)

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
|            Data Block            |
| vlen uint16 | value bytes []byte | ... 

Index
|                  Index Block                   |
| klen uint16 | key bytes []byte | offset uint32 | ...  

Metadata
| level uint16 | increment uint16 | indexStartPos uint32 |
```

WAL
``` 
klen uint16 | vlen uint 16 | key []byte | value []byte 
```

## Compaction Process

- Level based compaction (L0, L1) 
- Triggered at each memtable flush
- add level to file name ([level]_[incr].sst)
- L0 files can have overlapping keys but higher levels can't
- each sst write run compaction
- if number of sst files > threshold (default 3) : compaction
    - if L1 and sst range overlap L1 -> merge in L1
    - else -> create L1 whith the oldest incr in name
- keep ssts correctly ordered in the store


## Concurency

- Write wal log
- Flush memtable
- Compaction

## MVCC

MVCC transactions, inside the hood, the key is set with the commit ts.
- Get : key after txn start should not appear
- Get : retrieve last/max(ts) key
- Set : if value has been modified during the transaction -> abort
- Commit : Set ts and db.Set
- Rollback : abort txn 

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



Same config as before but with concurrent MemTable flush
```
goos: linux
goarch: amd64
pkg: froopydb/src
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkSet
BenchmarkSet-8            100000             17559 ns/op
BenchmarkGet
BenchmarkGet-8            100000               381.1 ns/op
PASS
ok      froopydb/src    1.935s
```
It's getting really interresting, lets add compaction concurrency.


From my last benchs, I'm quite happy. Big improvements might come from a new skiplist implementation.

## TODO

- [x] MemTable/log (skiplist)
- [x] Work with bytes arrays (keep keys as int in db call)
- [x] create file format for segments (rename SSTable)
- [x] Log bin format 
- [x] crash or start recovery WAL
- [x] crash or start recovery SStables
- [x] compaction process L0
- [x] compaction process L1
- [x] Add concurrency for Flush
- [x] Add concurrency for Wal Write
- [x] Add concurrency for Compaction
- [x] Clear split of files for better unit testing (wal, memtable, sst, db)
- [x] Continue split big files (wal, memtable, sst, db)
- [x] Add separeted parser for sstable (to dig)
- [x] clean error handling (too add when bored)
- [x] refactor method privacy (too add when bored)
- [x] add debug logger (create a simple one)
- [x] Split files for each sstable (data, index, metadata) -> everyting in one file is fine
- [x] Allow more or less than 4 bytes keys (maybe useless with types)
- [x] Improve sst.GetMinMax (maybe store min/max KeyOffset in metadata)
- [x] Create a clear api for user compaction (compaction should be called by the user)
- [x] DB stats queries (size, len ?, tables, memtableSize...)
- [x] Use a DBConfig object with a DefaultConfig
- [x] Improve benchs to have a clear idea on perfs
- [x] range queries
- [x] Skiplist custom
- [x] transactions/mvcc
    - [x] store ts in key (uint64)
    - [x] key|ts must be in memtable/sst/wal
- [ ] Better corrupted/crashed file recovery
- [ ] Add more unit testing (again)
- [ ] Improve compaction algo (multi level)
- [ ] Improve compaction perfs (minimal cpu usage)
- [ ] sst compression
- [ ] Bloom filter -> Should I still use in memory index or drop it to save memory ?
- [ ] A cool thing might be to type my key (str or time for now and maybe int, compaction shouldn't be call on a time based db)
- [ ] Create a new web (api, tcp event loop, grpc...?)
    - [ ] Bench through web api
    - [ ] Test concurent queries
- [ ] Study MMap potential use and benefits
- [x] Setup CI
- [ ] handle big values/keys
- [ ] Recode everything in Rust (or zig lol ?)
    - [ ] Do Python binding


## Usefull links
- https://github.com/Naelpuissant/mehdb/blob/main/document.pdf
- https://www.nan.fyi/database
- https://github.com/dgraph-io/badger/tree/main
- https://github.com/rosedblabs/wal/tree/main