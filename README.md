[![Go](https://github.com/Naelpuissant/FroopyDB/actions/workflows/go.yml/badge.svg)](https://github.com/Naelpuissant/FroopyDB/actions/workflows/go.yml)
![Coverage](https://img.shields.io/badge/Coverage-59.6%25-yellow)

# FroopyDB

A persistent LSM-tree based/key-value db for educational purpose


## File formats

SSTable
```
Data
|            Data Block            |
| vlen uint16 | value []byte | ... 

Index
|                  Index Block                   |
| klen uint16 | key []byte | offset uint32 | ...  

Index Start Offset
| keyOffset uint32 | ...  

Bloom Filter 
| bitmap []byte |

Metadata
| level uint16 | increment uint16 | nkeys uint32 | indexOffset uint32 | bloomFilterOffset uint32
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


```
goos: linux
goarch: amd64
pkg: froopydb
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkDBSet
BenchmarkDBSet-8          100000              7518 ns/op
BenchmarkDBGet
sstables 0
flush 0
BenchmarkDBGet-8          100000               987.7 ns/op
BenchmarkTxnSet
BenchmarkTxnSet-8         100000              6409 ns/op
BenchmarkTxnGet
BenchmarkTxnGet-8         100000              1088 ns/op
PASS
ok      froopydb        3.167s
```
Looks like we have really good improvements for DB Set, probably because of the new mutex free approach. Get has been increased, a cost due to the version checking (additionnal byte compare). 


```
goos: linux
goarch: amd64
pkg: froopydb
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkDBSet
BenchmarkDBSet-8          100000              5208 ns/op
BenchmarkDBGet
sstables 0
flush 0
sstables 1657
flush 0
BenchmarkDBGet-8          100000            263018 ns/op
BenchmarkTxnSet
BenchmarkTxnSet-8         100000              6031 ns/op
BenchmarkTxnGet
BenchmarkTxnGet-8         100000            670640 ns/op
PASS
ok      froopydb        97.306s
```
Something interesting to notice is that the Get doesn't scale well (good point that Set is stable btw), I find that when lowering the `MemTableMaxSize` (here to one KB) I got pretty bad performances, this is due to a higher number of sst. Now that I use skiplist as inmemory index cache, going for a specific key leads to a bad `O(t*n*log(n))` where t is the number of tables. Going back to a hashmap is not viable, it will leads to poor performance for version checking and it doesn't solve the actual high memory usage. A more viable solution would be to go deeper into "DB systems classics" by :
- adding a bloomfilter will drastically reduce the number sst lookups (t)
- removing skiplist inmemory index
- implementing a fast sst search (bisect)
- adding a lrucache (block cache)


```
goos: linux
goarch: amd64
pkg: froopydb
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkDBSet
BenchmarkDBSet-8          100000              4205 ns/op
BenchmarkDBGet
BenchmarkDBGet-8          100000             71323 ns/op
BenchmarkTxnSet
BenchmarkTxnSet-8         100000              6083 ns/op
BenchmarkTxnGet
BenchmarkTxnGet-8         100000            142439 ns/op
PASS
ok      froopydb        23.963s
```
With the combo skiplist and bloom filter we clearly improved. Note that we have a 1024 byte memtable right now, with a more realistic size we should be way better (with 10MB/5s bench we are arround 4000ns/op for DB and 5k for get and 8k for write on Txn)


```
goos: linux
goarch: amd64
pkg: froopydb
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkDBSet
BenchmarkDBSet-8          100000              2422 ns/op
BenchmarkDBGet
BenchmarkDBGet-8          100000             31916 ns/op
BenchmarkTxnSet
BenchmarkTxnSet-8         100000              4066 ns/op
BenchmarkTxnGet
BenchmarkTxnGet-8         100000              1515 ns/op
PASS
ok      froopydb        7.309s
```
Adding bisect scan we end up with way better perfs overall, didn't expect that since we also use way less memory which improve the db scalling (before we had 1 skiplist per sst, now we only read sst). Get looks still too high to me...

```
goos: linux
goarch: amd64
pkg: froopydb
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkDBSet
BenchmarkDBSet-8                  100000             11236 ns/op
BenchmarkDBGet
BenchmarkDBGet-8                  100000             91373 ns/op
BenchmarkTxnSet
BenchmarkTxnSet-8                 100000             13546 ns/op
BenchmarkTxnGet
BenchmarkTxnGet-8                 100000            149743 ns/op
BenchmarkTxnRandGet
BenchmarkTxnRandGet-8             100000            148971 ns/op
PASS
ok      froopydb        52.047s
```
Changed how my bench works, a bit scary but I'm ok with that since I shows clearly what need to be improved in pprof. For now still ok with Set because most of the job are down in parallel. For the get I still need to improve the bisect perfs and maybe monitor my bloom filter hit rate for exemple.

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
- [x] I wonder if it's viable to keep the index, massive clean and double check everything to have everything working, bench, start implementation without index lookup and with bloom filters (memory efficient)
    - [x] idea to make index work -> use skiplist (create new table on delete key, costly but only safe solution for now)
- [ ] Search massive rework (objective back to 1000ns/op)
    - [x] Bloom filter
      - [x] Create bloom filter
      - [x] Bloom filter persist/retrieve
      - [x] Use bloom filter before sst search  
    - [x] Check if key between sst min/max
    - [x] Bisect sst scan and remove skiplist inmemory index
        - [x] add IDX_NKEYS_SIZE and IDX_OFFSET_LIST_SIZE to metadata and provide the IDX_OFFSET_LIST
        - [x] implement bisect scan on SSTReader
        - [x] perf check
        - [x] update doc/readme
    - [ ] Perf check
        - [ ] Perf hint : On sst search, 1 read syscall per index, read the all index and search in it (might be a good candidate for mmap). 
    - [ ] Lrucache (start thinking about it, skip it for now if perfs are back to be 1000ns/op)
- [ ] Fix CI
- [ ] Bug : When spawning new db or restarting, old log file should be used, do not create new log file.
- [x] Put back background compaction jobs
- [x] Improve compaction algo (multi level)
- [ ] Clear db metrics and start real monitoring (expvar should do the job)
- [x] Fix and update benchs
- [ ] Have a proper manifest that allow me to restart db easily and to keep track of my compaction levels
- [ ] Better corrupted/crashed file recovery
- [ ] Zero copy
- [ ] DB iter (list all)
- [ ] Fix and add Range query to txn
- [ ] arena skiplist with cas instead of mutex lock
- [ ] Improve WAL (batch write, checksum...)
- [ ] sst compression (bring back level compression)
- [ ] Allow transactionless operations (no conflict checking)
- [ ] A cool thing might be to type my key (str or time for now and maybe int, compaction shouldn't be call on a time based db, int db should benefit from skiplist faster compare)
- [ ] Study MMap potential uses and benefits
- [ ] Improve compaction perfs (minimal cpu usage, study gc algorithms)
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