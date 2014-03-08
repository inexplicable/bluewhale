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
* Checksum document factory as an option

# Developer Notes:
* The primary API of the bluewhale caching is `org.ebaysf.bluewhale.Cache` and `org.ebaysf.bluewhale.configurable.CacheBuilder` similar to `com.google.common.cache.*`
* Functional wise, bluewhale caching is mostly compliant with Guava's Cache, but you must provide the key/value `org.ebaysf.bluewhale.serialization.Serializer` in addition.
* And there's various configurations you could tune based on `org.ebaysf.bluewhale.configurable.Configuration`, more details will be explained below.
* The structure of bluewhale caching is simple, a `RangeMap<Integer, Segment>`, and a `RangeMap<Integer, BinJournal>` at the essence of it.
* A `Segment` is conceptually a `long[]`, hashCode is broken to `segmentCode` and `segmentOffset` to fetch a long value from the corresponding `Segment`.
* The long value is broken to `journalCode` and `journalOffset` similarly, pointing to a `BinDocument` stored in some `BinJournal`.
* The `BinDocument` read contains the serialized `key`, `value`, `hashCode`, etc. which requires the same `Serializer` to deserialize it to the expected value type.

# Configurations:
* __key__ `Serializer` must be provided, check `org.ebaysf.bluewhale.serialization.Serializers` for existing types' support.
* __value__ `Serializer` must be provided.
* __concurrencyLevel__ `int` manages the number of `Segment` to be initialized, default value is `2`, which creates `2 << 3 = 8` segments
* __maxSegmentDepth__ `int` manages the width of `Segment`, default value is `2`, which means each `Segment` initialized cannot be splitted more than `2` times
* __maxPathDepth__ `int` manages the depth of `Path`, default value is `7`, which triggers a `Path` shorten request whenever a `Path` is deeper than `7`
* __factory__ `BinDocumentFactory` is the factory class to create `BinDocument`s, default is `BinDocumentFactories.RAW` which creates `BinDocumentRaw` implementations with no checksums.
* __journalLength__ `int` manages the bytes of each journal file, default is `1 << 29 = 512MB`, it must be postive but not over `Integer.MAX_VALUE`.
* __maxJournals__ `int` manages the total number of journal files the cache will maintain, default is `8 => 4G`, whenever more journal files are present, eviction must happen to retire old journals.
* __maxMemoryMappedJournals__ `int` manages the total number of RAM we will use as memory mapped files, default is `2 => 1G`, whenever more memory mapped journals are present, downgrade must happen, check `org.ebaysf.bluewhale.storage.BinStorageImpl#downgrade` for details.
* __leastJournalUsageRatio__ `float` manages the threshold of journal compaction, default is `0.1f`, when a journal's usage ratio is below or equal to this value, this journal will be compacted.
* __dangerousJournalsRatio__ `float` manages the threshold of LRU eviction strategy triggering, default is `0.25f`, when the cache is LRU configured, and the `BinDocument` read is in dangerous journals, it will be refreshed, check `org.ebaysf.bluewhale.storage.UsageTrack#refresh`.
* __ttl__ `Pair<Long, TimeUnit>` manages the expiration of journals, default is `null`, means it never expires. Otherwise, a journal will be evicted if it's older than `now - TimeUnit.toNanos(Long)`.
* __evictionStrategy__ `EvictionStrategy` manages the eviction policy before cache maxed out, default is `EvictionStrategy.SILENCE` which is effectively LRW, `EvictionStrategy.LRU` is an alternative.
* __eventBus__ `EventBus` manages the event handling, default is `new com.google.common.eventbus.EventBus()`.
* __executor__ `ExecutorService` manages the async jobs processing, default is `java.util.concurrent.Executors.newCachedThreadPool()`.
* __local__ `File` manages the directory of the cache to put its `Segment`, `BinJournal` files, default is `com.google.common.io.Files.newTempDir()`.
* __cold__ `File` manages the source/destination of the cold cache to be loaded/persisted, default is `null`, means no persistence will be done.
* __persistent__ `boolean` manages the persistence, when __cold__ is enabled, it must be `true`, default is `false`, and the `Segment`, `BinJournal` files will be deleted on JVM exit.

