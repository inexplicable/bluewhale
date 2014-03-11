bluewhale
=========

This is a Guava compliant caching implementation, mainly focused on large volume in process caching with minimal GC overhead.
It's based on MemoryMapped files, and inspired by Bitcask (Erlang) & LevelDB (C++) while remaining purely in Java.

The sequential writes allow us to get write performance around 3μs for 100bytes key/value. The lock free reads allow us to get read performance around 2μs whether sequential or random

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
readrandom   :     1.80890 micros/op;   61.2 MB/s
readseq      :     1.09180 micros/op;  101.3 MB/s
```

# Followings are the supported features:
* Guava Cache API supported features except for #asMap (guess why, and you do have CacheBuilder, TTL, stats, removal notification etc.)
* LRW | LRU eviction strategy
* RAM, Disk consumption constraints
* Transient or Persistent caching based on configuration
* Data Integrity protection via optional binary document checksum

# Developer Notes:
* The primary API of the bluewhale caching is `org.ebaysf.bluewhale.Cache` and `org.ebaysf.bluewhale.configurable.CacheBuilder` similar to `com.google.common.cache.*`
* Functional wise, bluewhale caching is mostly compliant with Guava's Cache, but you must provide the key/value `org.ebaysf.bluewhale.serialization.Serializer` in addition.
* And there's various configurations you could tune based on `org.ebaysf.bluewhale.configurable.Configuration`, more details will be explained below.
* The structure of bluewhale caching is simple, a `com.google.common.collect.RangeMap<Integer, Segment>`, and a `com.google.common.collect.RangeMap<Integer, BinJournal>` at the essence of it.
* A `Segment` is conceptually a `long[]`, hashCode is broken to `segmentCode` and `segmentOffset` to fetch a long value from the corresponding `Segment`.
* The long value is broken to `journalCode` and `journalOffset` similarly, pointing to a `BinDocument` stored in some `BinJournal`.
* The `BinDocument` read contains the serialized `key`, `value`, `hashCode`, etc. which requires the same `Serializer` to deserialize it to the expected value type.
* Cold cache is supported via `org.ebaysf.bluewhale.persistence.*`, using GSON to manifest `Segment` and `Journal` in readable json format, and allow a load back from the same file it writes to whenever there's any structural change.

# Configurations:
* __key__ `Serializer` must be provided, check `org.ebaysf.bluewhale.serialization.Serializers` for existing types' support.
* __value__ `Serializer` must be provided.
* __concurrencyLevel__ `int` manages the number of `Segment` to be initialized, default value is `3`, which creates `2 << 3 = 8` segments
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

# Tunings:
* There're various tunings we could do given the configurations above, the most effective, and costly would be to allocate more RAM, using __maxMemoryMappedJournals__
* From `2` to `4`, which effectively increased the RAM allocation from `1G` to `2G`, the very same benchmark result becomes the following:

```
fillseq      :     0.89166 micros/op;  124.1 MB/s
fillseq      :     1.33815 micros/op;   82.7 MB/s
fillsync     :     2.36350 micros/op;   46.8 MB/s (10000 ops)
fillrandom   :     2.06842 micros/op;   53.5 MB/s
fillseq      :     2.38430 micros/op;   46.4 MB/s
overwrite    :     1.30755 micros/op;   84.6 MB/s
fillseq      :     1.50557 micros/op;   73.5 MB/s
readseq      :     0.41730 micros/op;  265.1 MB/s
readrandom   :     0.93344 micros/op;  118.5 MB/s
readrandom   :     0.95614 micros/op;  115.7 MB/s
readseq      :     0.41751 micros/op;  265.0 MB/s
readrandom   :     0.96796 micros/op;  114.3 MB/s
readseq      :     0.42836 micros/op;  258.3 MB/s
```