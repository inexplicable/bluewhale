bluewhale
=========

This is a Guava compliant caching implementation, mainly focused on larger volume of local caching without painful JVM GCs.
It's based on MemoryMapped files, and inspired by Bitcask (Erlang) & LevelDB (C++) while remaining purely in Java.

The sequential writes allow us to get write performance around 1ms for 100bytes key/value
The lock free reads allow us to get read performance around 1ms whether sequential or random

Yet production ready, work in progress @@

