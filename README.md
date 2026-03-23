[![Go](https://github.com/Naelpuissant/FroopyDB/actions/workflows/go.yml/badge.svg)](https://github.com/Naelpuissant/FroopyDB/actions/workflows/go.yml)
![Coverage](https://img.shields.io/badge/Coverage-56.1%25-yellow)

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

> **Important:** TO BE UPDATED

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

- Write wal log in a channel
- Flush memtable in a channel
- Skiplist mutexed
- SST write ordered in a channel (flush/compaction)
- immutable memtables atomic pointer + copy on write (CoW)
- SSTables store atomic pointer + copy on write (CoW)

### Flush workflow

For my flush workflow I'm using copy on write (COW) and compare and swap (CAS) strategy to avoid locking and allowing concurrent reads while flushing. This implies having immutables memtables and immutables sst store, trying to swap with optimistic locking. 



DB Tables state (`froopydb.Tables`) :
- mem : current memtable
- imm : immutable memtable (currently flushing, immutable)
- sst : sst store (immutable)

Should flush : 
- create a new memtable
- copy imm to new imm
- append mem to new imm
- CAS
- on success : set key to new mem and start flush job

Flush Job :
- create new sst table (not store)
- flush oldest mem to new sst
- copy sst (store) and append new sst
- remove old mem from imm
- CAS
- on success : Done

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
- [x] Setup CI
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
- [x] Global cleanup 
    - [x] clean db api -> we should only call txn
    - [x] have a clear api for search/get (return (bytes and found))
- [x] check concurrency safety
    - [x] Copy on Write for immutable memtables
    - [x] Copy on Write for sst store
- [x] Just quick check mvcc on get from sst (hell nah)
- [ ] I wonder if it's viable to keep the index, massive clean and double check everything to have everything working, bench, start implementation without index lookup and with bloom filters (memory efficient)
    - [x] idea to make index work -> use skiplist (create new table on delete key, costly but only safe solution for now)
- [ ] Bloom filter -> Should I still use in memory index or drop it to save memory ?
- [x] Put back background compaction jobs
- [x] Improve compaction algo (multi level)
- [ ] Clear db metrics
- [ ] Fix and update benchs
- [ ] Have a proper manifest that allow me to restart db easily and to keep track of my compaction levels
- [ ] Better corrupted/crashed file recovery
- [ ] Add Range query to txn
- [ ] arena skiplist with cas instead of mutex lock
- [ ] sst compression
- [ ] Improve WAL (batch write...)
- [ ] Allow transactionless operations (no conflict checking)
- [ ] A cool thing might be to type my key (str or time for now and maybe int, compaction shouldn't be call on a time based db, int db should benefit from skiplist faster compare)
- [ ] Study MMap potential use and benefits
- [ ] add caching
- [ ] Improve compaction perfs (minimal cpu usage)
- [ ] Create a new web (api, tcp event loop, grpc...?)
    - [ ] Bench through web api
    - [ ] Test concurent queries
- [ ] handle big values/keys
- [ ] Recode everything in Rust (or zig lol ?)
    - [ ] Do Python binding


## Usefull links
- https://github.com/Naelpuissant/mehdb/blob/main/document.pdf
- https://www.nan.fyi/database
- https://github.com/dgraph-io/badger/tree/main
- https://github.com/rosedblabs/wal/tree/main
- https://info.varnish-software.com/blog/how-memory-maps-mmap-deliver-25x-faster-file-access-in-go?utm_source=tldrwebdev
- https://www.bitflux.ai/blog/memory-is-slow-part2/?utm_source=tldrwebdev