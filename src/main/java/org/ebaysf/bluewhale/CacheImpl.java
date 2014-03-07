package org.ebaysf.bluewhale;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.ebaysf.bluewhale.command.GetImpl;
import org.ebaysf.bluewhale.command.PutAsInvalidate;
import org.ebaysf.bluewhale.command.PutAsIs;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.event.PostInvalidateAllEvent;
import org.ebaysf.bluewhale.event.PostSegmentSplitEvent;
import org.ebaysf.bluewhale.event.RemovalNotificationEvent;
import org.ebaysf.bluewhale.event.SegmentSplitEvent;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.segment.SegmentsManager;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.ebaysf.bluewhale.storage.BinStorageImpl;
import org.ebaysf.bluewhale.storage.UsageTrack;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Created by huzhou on 2/28/14.
 */
public class CacheImpl <K, V> extends AbstractCache<K, V> implements Cache<K, V>, UsageTrack {

    private static final Logger LOG = Logger.getLogger(CacheImpl.class.getName());

    private final Configuration _configuration;
    private final transient SegmentsManager _manager;
    private final BinStorage _storage;
    private final transient StatsCounter _statsCounter;

    private final transient RemovalListener<K, V> _removalListener;

    protected volatile RangeMap<Integer, Segment> _navigableSegments;

    public CacheImpl(final Configuration configuration,
                     final RemovalListener<K, V> removalListener,
                     final List<Segment> coldSegments,
                     final List<BinJournal> coldJournals) throws IOException {

        _configuration = Preconditions.checkNotNull(configuration);
        _removalListener = Preconditions.checkNotNull(removalListener);

        _manager = new SegmentsManager(_configuration);
        _storage = new BinStorageImpl(_configuration, coldJournals, this);
        _statsCounter = new SimpleStatsCounter();

        _configuration.getEventBus().register(this);

        _navigableSegments = _manager.initSegments(
                Objects.firstNonNull(coldSegments, Collections.<Segment>emptyList()), _storage);
    }

    public @Override Serializer<K> getKeySerializer() {

        return _configuration.getKeySerializer();
    }

    public @Override Serializer<V> getValSerializer() {

        return _configuration.getValSerializer();
    }

    public @Override V getIfPresent(Object key) {

        Preconditions.checkArgument(key != null);

        final int hashCode = getKeySerializer().hashCode((K)key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try {
            return zone.get(new GetImpl(key, null, hashCode, false, _statsCounter));
        }
        catch (Exception ex) {
            LOG.warning(Throwables.getStackTraceAsString(ex));
            return null;
        }
    }

    public @Override V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {

        Preconditions.checkArgument(key != null && valueLoader != null);

        final int hashCode = getKeySerializer().hashCode((K) key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try {
            return zone.get(new GetImpl(key, valueLoader, hashCode, true, _statsCounter));
        }
        catch (Exception ex) {
            LOG.warning(Throwables.getStackTraceAsString(ex));
            return null;
        }
    }

    public @Override void put(K key, V value) {

        Preconditions.checkArgument(key != null && value != null);

        final int hashCode = getKeySerializer().hashCode((K)key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try {
            zone.put(new PutAsIs(key, value, hashCode, System.nanoTime()));
        }
        catch (Exception ex) {
            LOG.warning(Throwables.getStackTraceAsString(ex));
        }
    }

    public @Override void invalidate(Object key) {

        Preconditions.checkArgument(key != null);

        final int hashCode = getKeySerializer().hashCode((K)key);
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        try{
            zone.put(new PutAsInvalidate(key, hashCode, System.nanoTime()));
        }
        catch (Exception ex) {
            LOG.warning(Throwables.getStackTraceAsString(ex));
        }
    }

    public @Override void invalidateAll() {

        final Collection<Segment> abandons = _navigableSegments.asMapOfRanges().values();

        try {
            //the fastest way to invalidate everything is to wipe the segments clean.
            _navigableSegments = _manager.initSegments(Collections.<Segment>emptyList(), _storage);
        }
        catch (IOException e) {
            LOG.warning(Throwables.getStackTraceAsString(e));
        }

        _configuration.getEventBus().post(new PostInvalidateAllEvent(abandons, this));
    }

    public @Override long size() {

        long size = 0L;

        for(Segment segment : _navigableSegments.asMapOfRanges().values()){
            size += segment.size();
        }

        return size;
    }

    public @Override CacheStats stats() {

        return _statsCounter.snapshot();
    }

    public @Override boolean using(final BinDocument document) {

        final int hashCode = document.getHashCode();
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        return zone.using(document);
    }

    public @Override void forget(final BinDocument document, final RemovalCause cause) {

        final int hashCode = document.getHashCode();
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        zone.forget(document, cause);
    }

    public @Override void refresh(final BinDocument document) {

        final int hashCode = document.getHashCode();
        final int segmentCode = getSegmentCode(hashCode);
        final Segment zone = route(segmentCode);

        zone.refresh(document);
    }

    protected Segment route(final int segmentCode) {

        return _navigableSegments.get(segmentCode);
    }

    @Subscribe
    public void onSegmentSplit(final SegmentSplitEvent event){

        final Segment before = event.getSource();

        final ImmutableRangeMap.Builder<Integer, Segment> modifying = ImmutableRangeMap.builder();
        for(Map.Entry<Range<Integer>, Segment> entry : _navigableSegments.asMapOfRanges().entrySet()){
            if(!Objects.equal(before.range(), entry.getKey())){
                modifying.put(entry.getKey(), entry.getValue());
            }
        }

        final List<Segment> after = event.after();
        for(Segment child : after){
            try{
                modifying.put(child.range(), child);
            }
            catch(Exception e){
                LOG.warning(Throwables.getStackTraceAsString(e));
            }
        }

        _navigableSegments = modifying.build();

        _configuration.getEventBus().post(new PostSegmentSplitEvent(before));
    }

    @Subscribe
    @AllowConcurrentEvents
    public void onRemovalNotification(final RemovalNotificationEvent event){

        final BinDocument document = event.getSource();
        final RemovalCause cause = event.getRemovalCase();

        final K key = getKeySerializer().deserialize(document.getKey(), false);

        _statsCounter.recordEviction();
        _removalListener.onRemoval(new RemovalNotificationOverBuffer<K, V>(key, document, getValSerializer(), cause));
    }

    public static int getSegmentCode(final int hashCode) {
        return hashCode >>> 16;//this gives us the highest 16 bits as segment code (int)
    }
}
