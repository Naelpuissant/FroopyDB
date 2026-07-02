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

> **Important:** NOT UP TO DATE SINCE COMPACTION REFACTOR, 
CURRENTLY NO LEVEL BASED COMPACTION

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

## Iterator (wip)

Naive implementation :
- inside txn
- sort tables by min values
- get min value from k sorted tables
- from min value table, search for next
- from min value tables where min key <= curr min key, search for next
- keep best candidate like `maxTs(curr.key <= next.key & next.ts < txn.ts)`

notes : 
- No copy (unless user asked it)
- 

hint for next improvements :
- min heap with & streaming
- block streaming

## Bloc cache (wip)

naive implementation :
- lru cache
- fixed sized bloc (bytes)
- prefix compaction (shared prefix as cache key, only suffixes stored)
ex :
```
pre|suffix
133
    67 -> 13367 
    68 -> 13368
    ...
```
## Bench

Benchmark history and notes : [BENCH.md](BENCH.md).

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
- [ ] Search massive rework
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
    - [ ] Perf check -> still not that good
        - [x] Perf hint 1 : On sst search, 1 read syscall per index, read the all index and search in it (might be a good candidate for mmap).- -> Doesn't looks satisfying, still too much reads syscalls, block bin search should be the key.
        - [x] Perf hint 1 : MMap the entire SST -> lol this is huge (~2k ns/ops)
        - [ ] Perf hint 1.5 : When looping, we don't want to heap allocate on each iteration, to fix that, only return key start/end offsets and only create object once.
        - [ ] Perf hint 2 : Block approach, block (first key len, first key, offset), search block, then search inside block
        - [ ] Perf hint 3 : Block lru cache
        - [ ] Perf hint 3.5 : maybe adding a bloom filter on block might be good
- [ ] Bug : When spawning new db or restarting, old log file should be used, do not create new log file. -> must be fixed when I'll add manifest
- [ ] Bug : Sometime compaction test fails, investigate
- [ ] Study what are the impacts of concurents process and maybe add a file locking system
    - having multiple processes, does it makes sense since lsm inmemory table wouldn't be accessible ?
        - Yes since we can have the same process spawning multiple db concurently
- [x] Put back background compaction jobs
- [x] Improve compaction algo (multi level)
- [ ] Clear db metrics and start real monitoring (expvar should do the job)
- [x] Fix and update benchs
- [ ] DB iter (list all)
- [ ] Have a proper manifest that allow me to restart db easily and to keep track of my compaction levels
- [ ] Better corrupted/crashed file recovery
- [ ] Value zero copy on get
- [ ] Fix and add Range query to txn
- [ ] arena skiplist with cas instead of mutex lock
- [ ] Improve WAL (batch write, checksum...)
- [ ] sst compression (bring back level compression)
- [ ] Allow transactionless operations (no conflict checking)
- [ ] A cool thing might be to type my key (str or time for now and maybe int, compaction shouldn't be call on a time based db, int db should benefit from skiplist faster compare)
- [x] Study MMap potential uses and benefits
- [ ] Study HLL for approximate key counts
- [ ] Study add index (and structured value mode -> NoSQL)
- [ ] Improve compaction perfs (minimal cpu usage, study gc algorithms)
- [ ] Create a new web (api, tcp event loop, grpc...?)
    - [ ] Bench through web api
    - [ ] Test concurent queries
- [ ] handle big values/keys
- [ ] Recode everything in Rust (or zig lol ?)
    - [ ] Do Python binding


## References
- https://www.nan.fyi/database
- https://github.com/dgraph-io/badger/tree/main
- https://github.com/rosedblabs/wal/tree/main
- https://info.varnish-software.com/blog/how-memory-maps-mmap-deliver-25x-faster-file-access-in-go?utm_source=tldrwebdev
- https://www.bitflux.ai/blog/memory-is-slow-part2/?utm_source=tldrwebdev