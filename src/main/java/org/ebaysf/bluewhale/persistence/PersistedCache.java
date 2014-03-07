package org.ebaysf.bluewhale.persistence;

import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinJournal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * Created by huzhou on 3/7/14.
 */
public class PersistedCache<K, V> implements Cache<K, V> {

    private final Configuration _configuration;
    private final List<Segment> _navigableSegments;
    private final PersistedStorage _storage;

    public PersistedCache(final Configuration configuration,
                          final List<Segment> segments,
                          final PersistedStorage storage){

        _configuration = configuration;
        _navigableSegments = segments;
        _storage = storage;
    }
    
    public @Override Configuration getConfiguration() {
        return _configuration;
    }

    public List<Segment> getPersistedSegments() {
        return Lists.newArrayList(_navigableSegments);
    }

    public List<BinJournal> getPersistedJournals() {
        final List<BinJournal> journals = Lists.newLinkedList();
        Iterators.addAll(journals, _storage.iterator());
        return journals;
    }

    public @Override Serializer<K> getKeySerializer() {
        throw new UnsupportedOperationException();
    }

    public @Override Serializer<V> getValSerializer() {
        throw new UnsupportedOperationException();
    }

    public @Override V getIfPresent(Object key) {
        throw new UnsupportedOperationException();
    }

    public @Override V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
        throw new UnsupportedOperationException();
    }

    public @Override ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
        throw new UnsupportedOperationException();
    }

    public @Override void put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    public @Override void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    public @Override void invalidate(Object key) {
        throw new UnsupportedOperationException();
    }

    public @Override void invalidateAll(Iterable<?> keys) {
        throw new UnsupportedOperationException();
    }

    public @Override void invalidateAll() {
        throw new UnsupportedOperationException();
    }

    public @Override long size() {
        throw new UnsupportedOperationException();
    }

    public @Override CacheStats stats() {
        throw new UnsupportedOperationException();
    }

    public @Override ConcurrentMap<K, V> asMap() {
        throw new UnsupportedOperationException();
    }

    public @Override void cleanUp() {
        throw new UnsupportedOperationException();
    }
}
