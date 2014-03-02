package org.ebaysf.bluewhale;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.ebaysf.bluewhale.command.GetImpl;
import org.ebaysf.bluewhale.command.PutAsInvalidate;
import org.ebaysf.bluewhale.command.PutImpl;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.event.PostInvalidateAllEvent;
import org.ebaysf.bluewhale.event.PostSegmentSplitEvent;
import org.ebaysf.bluewhale.event.SegmentSplitEvent;
import org.ebaysf.bluewhale.segment.LeafSegment;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.segment.SegmentsManager;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.ebaysf.bluewhale.storage.BinStorageImpl;
import org.ebaysf.bluewhale.storage.UsageTrack;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * Created by huzhou on 2/28/14.
 */
public class CacheImpl <K, V> implements Cache<K, V>, UsageTrack {

    private final File _local;
    private final int _concurrencyLevel;
    private final Serializer<K> _keySerializer;
    private final Serializer<V> _valSerializer;
    private final SegmentsManager _manager;
    private final BinStorage _storage;

    private final EventBus _eventBus;
    private final ListeningExecutorService _executor;

    protected volatile RangeMap<Integer, Segment> _navigableSegments;

    public CacheImpl(final File local,
                     final int concurrencyLevel,
                     final Serializer<K> keySerializer,
                     final Serializer<V> valSerializer,
                     final EventBus eventBus,
                     final ListeningExecutorService executor,
                     final BinDocumentFactory factory,
                     final int journalLength,
                     final int maxJournals,
                     final int maxMemoryMappedJournals,
                     final List<BinJournal> loadings) throws IOException {

        _local = Preconditions.checkNotNull(local);
        _concurrencyLevel = concurrencyLevel;Preconditions.checkArgument(concurrencyLevel > 0 && concurrencyLevel < 16);
        _keySerializer = Preconditions.checkNotNull(keySerializer);
        _valSerializer = Preconditions.checkNotNull(valSerializer);
        _eventBus = Preconditions.checkNotNull(eventBus);
        _executor = Preconditions.checkNotNull(executor);

        _manager = new SegmentsManager(local, this);
        _storage = new BinStorageImpl(local, factory, journalLength, maxJournals,
                maxMemoryMappedJournals, loadings, eventBus, executor, this);

        _eventBus.register(this);

        _navigableSegments = initSegments(_local, concurrencyLevel);
    }

    @Override
    public EventBus getEventBus() {

        return _eventBus;
    }

    @Override
    public ListeningExecutorService getExecutor() {

        return _executor;
    }

    @Override
    public Serializer<K> getKeySerializer() {

        return _keySerializer;
    }

    @Override
    public Serializer<V> getValSerializer() {

        return _valSerializer;
    }

    @Override
    public BinStorage getStorage() {

        return _storage;
    }


    @Override
    public V getIfPresent(Object key) {

        Preconditions.checkArgument(key != null);

        final int hashCode = getKeySerializer().hashCode((K)key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try {

            return zone.get(new GetImpl(key, null, hashCode, false));
        }
        catch (Exception ex) {

            return null;
        }
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {

        Preconditions.checkArgument(key != null && valueLoader != null);

        final int hashCode = getKeySerializer().hashCode((K)key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try {

            return zone.get(new GetImpl(key, valueLoader, hashCode, true));
        }
        catch (Exception ex) {

            return null;
        }
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {

        Preconditions.checkArgument(keys != null);

        final ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();

        for(Object key : keys){
            builder.put((K)key, getIfPresent(key));
        }

        return builder.build();
    }

    @Override
    public void put(K key, V value) {

        Preconditions.checkArgument(key != null && value != null);

        final int hashCode = getKeySerializer().hashCode((K)key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try {

            zone.put(new PutImpl(key, value, hashCode, System.nanoTime()));
        }
        catch (Exception ex) {

        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

        Preconditions.checkArgument(m != null);

        for(Map.Entry<? extends K, ? extends V> entry : m.entrySet()){
            put(entry.getKey(), entry.getValue());
        }

    }

    @Override
    public void invalidate(Object key) {

        Preconditions.checkArgument(key != null);

        final int hashCode = getKeySerializer().hashCode((K)key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try{

            zone.put(new PutAsInvalidate(key, hashCode, System.nanoTime()));
        }
        catch (Exception ex) {

        }
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {

        Preconditions.checkArgument(keys != null);

        for(Object key : keys){

            invalidate(key);
        }
    }

    @Override
    public void invalidateAll() {

        final Collection<Segment> abandons = _navigableSegments.asMapOfRanges().values();

        try {
            //the fastest way to invalidate everything is to wipe the segments clean.
            _navigableSegments = initSegments(_local, _concurrencyLevel);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        getEventBus().post(new PostInvalidateAllEvent(abandons, this));
    }

    @Override
    public long size() {

        long size = 0L;

        for(Segment segment : _navigableSegments.asMapOfRanges().values()){
            size += segment.size();
        }

        return size;
    }

    @Override
    public CacheStats stats() {
        return null;
    }

    @Override
    public ConcurrentMap<K, V> asMap() {

        throw new UnsupportedOperationException("You don't really want a huge map like this :)");
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public boolean using(final BinDocument document) {

        final int hashCode = document.getHashCode();
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        return zone.using(document);
    }

    protected Segment route(final int segmentCode) {

        return _navigableSegments.get(segmentCode);
    }

    protected RangeMap<Integer, Segment> initSegments(final File dir, final int concurrencyLevel) throws IOException {

        final int span = Segment.MAX_SEGMENTS >> concurrencyLevel;
        final ImmutableRangeMap.Builder<Integer, Segment> builder = ImmutableRangeMap.builder();

        for(int lowerBound = 0, upperBound = lowerBound + span - 1; lowerBound < Segment.MAX_SEGMENTS; lowerBound += span, upperBound += span){
            final Range<Integer> range = Range.closed(lowerBound, upperBound);
            builder.put(range,
                    new LeafSegment(range, this, _manager, _manager.allocateBuffer()));
        }

        return builder.build();
    }

    @Subscribe
    protected void onSegmentSplit(final SegmentSplitEvent event){

        final Segment before = event.before();

        final ImmutableRangeMap.Builder<Integer, Segment> modifying = ImmutableRangeMap.builder();
        for(Map.Entry<Range<Integer>, Segment> entry : _navigableSegments.asMapOfRanges().entrySet()){
            if(!Objects.equals(before.range(), entry.getKey())){
                modifying.put(entry.getKey(), entry.getValue());
            }
        }

        final List<Segment> after = event.after();
        for(Segment child : after){
            modifying.put(child.range(), child);
        }

        _navigableSegments = modifying.build();

        getEventBus().post(new PostSegmentSplitEvent(before));
    }

    public static int getSegmentCode(final int hashCode) {
        return hashCode >>> 16;//this gives us the highest 16 bits as segment code (int)
    }
}
