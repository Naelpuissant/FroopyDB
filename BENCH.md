# Benchmarks

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
Changed how my bench works, a bit scary but I'm ok with that since it shows clearly what need to be improved in pprof. For now still ok with Set because most of the job are down in parallel. For the Get I still need to improve the bisect perfs and maybe monitor my bloom filter hit rate.


```
goos: linux
goarch: amd64
pkg: froopydb
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkDBSet
BenchmarkDBSet-8                  100000              7044 ns/op
BenchmarkDBGet
BenchmarkDBGet-8                  100000              2261 ns/op
BenchmarkTxnSet
BenchmarkTxnSet-8                 100000              8618 ns/op
BenchmarkTxnGet
BenchmarkTxnGet-8                 100000              3726 ns/op
BenchmarkTxnRandGet
BenchmarkTxnRandGet-8             100000              3914 ns/op
PASS
ok      froopydb        3.016s
```
After using mmap on sst reader, we are back with reasonable perfs.
Can still be improved but fine for now.
