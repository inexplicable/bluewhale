bluewhale
=========

This is a Guava compliant caching implementation, mainly focused on larger volume of local caching without painful JVM GCs.
It's based on MemoryMapped files, and inspired by Bitcask (Erlang) & LevelDB (C++) while remaining purely in Java.

The sequential writes allow us to get write performance around 1ms for 100bytes key/value
The lock free reads allow us to get read performance around 1ms whether sequential or random

Yet production ready, try at your own risk; Well, first batch of results coming out:

```
LevelDB:    iq80 leveldb version 0.4
Date:       Wed Mar 05 16:41:13 PST 2014
Keys:       16 bytes each
Values:     100 bytes each (50 bytes after compression)
Entries:    10000000
RawSize:    1106.3 MB (estimated)
FileSize:   629.4 MB (estimated)
------------------------------------------------
fillseq      :     0.93751 micros/op;  118.0 MB/s
fillseq      :     2.59594 micros/op;   42.6 MB/s
fillsync     :     5.31380 micros/op;   20.8 MB/s (10000 ops)
fillrandom   :     2.87762 micros/op;   38.4 MB/s
fillseq      :     3.56622 micros/op;   31.0 MB/s
overwrite    :     2.89620 micros/op;   38.2 MB/s
fillseq      :     2.92466 micros/op;   37.8 MB/s
readseq      :     1.33068 micros/op;   83.1 MB/s
readrandom   :     1.79489 micros/op;   61.6 MB/s
readrandom   :     1.78563 micros/op;   62.0 MB/s
readseq      :     1.08474 micros/op;  102.0 MB/s
compact      :     9.00000 micros/op; 
readrandom   :     1.80890 micros/op;   61.2 MB/s
readseq      :     1.09180 micros/op;  101.3 MB/s
```

# Followings are the supported features:
* Guava Cache API supported features except for #asMap (guess why)
* RemovalNotification
* Max Journals (storage) limit
* Max MemoryMapped Journals (RAM) limit
* Max Segment Depth (concurrency) limit
* Eviction when size maxed out (LRW)
* Compression and compaction of old journals
* Files cleanup on close
* Simple Stats
* LRU and other injectable eviction strategy
* TTL
* Cache Builder semantics
* Cold cache

# Upcoming features:
* Checksum document factory as an option

```
LevelDB:    iq80 leveldb version 0.4
Date:       Thu Mar 06 13:55:25 PST 2014
Keys:       16 bytes each
Values:     100 bytes each (50 bytes after compression)
Entries:    10000000
RawSize:    1106.3 MB (estimated)
FileSize:   629.4 MB (estimated)
------------------------------------------------
fillseq      :     0.90590 micros/op;  122.1 MB/s
fillseq      :     2.54747 micros/op;   43.4 MB/s
fillsync     :     3.93100 micros/op;   28.1 MB/s (10000 ops)
fillrandom   :     2.85027 micros/op;   38.8 MB/s
fillseq      :     3.19238 micros/op;   34.7 MB/s
overwrite    :     2.48910 micros/op;   44.4 MB/s
fillseq      :     2.61987 micros/op;   42.2 MB/s
readseq      :     1.11218 micros/op;   99.5 MB/s
readrandom   :     1.81675 micros/op;   60.9 MB/s
readrandom   :     1.81146 micros/op;   61.1 MB/s
readseq      :     1.10340 micros/op;  100.3 MB/s
compact      :     9.00000 micros/op; 
readrandom   :     1.79900 micros/op;   61.5 MB/s
readseq      :     1.10308 micros/op;  100.3 MB/s
```
