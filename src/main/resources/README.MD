Why the 2nd version?

Mainly to address the followings:

# Minimize dependencies from `Journal` to `Segment` using `UsageTrack` interface.
# LRU/LRW and more `EvictionStrategy` support with efficiency.
# Less stress on CPU by reducing the async tasks.
    * This is closely align with our VM's spec to be exact, we have too many threads already competing for CPU resources, while there're redundant RAM untouched by the JVM waiting for exploration.
    * The decision is to make a trade-off, using a bit more RAM in a controlled manner, and use cached threadpool instead.
# Consistent `Segment` routing and `Journal` routing.
# Simplification over Performance in a lot of cases.
    * Leverage `com.google.common.collect.RangeMap` to route `Segment` and `Journal`.
    * Leverage `com.google.common.eventbus.EventBus` to decouple listeners.
    * Using `Serializer#equals` to avoid `BinComparableLeafSegment` completely.
    * Consolidate `Put` subtypes into less in total.
# Separate `BinDocumentWithChecksum` from `BinDocumentRaw`.
# Better boundary protection over max out `Segment`, `Journal`
    * `Journal` must be rotated.
    * Rotation must be logged.
    * Manifest must be flushed at each segment splits.
    * Cold cache loading should honor the rotation logged.
    * Cold cache loading should honor max numbers changes if any.
# Better use of generics, eliminate wherever unneeded.
# Persistence using `Gson` that is more readable.
