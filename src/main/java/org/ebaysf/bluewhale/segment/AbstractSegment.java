package org.ebaysf.bluewhale.segment;

import com.google.common.collect.Range;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.command.Get;
import org.ebaysf.bluewhale.command.Put;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huzhou on 2/28/14.
 */
public abstract class AbstractSegment implements Segment {

    private final Range<Integer> _range;

    protected final Cache<?, ?> _belongsTo;
    protected final SegmentsManager _manager;
    protected final ReentrantLock _lock;

    protected volatile int _size;
    protected volatile Segment _lower;
    protected volatile Segment _upper;

    public AbstractSegment(final Range<Integer> range,
                           final Cache<?, ?> belongsTo,
                           final SegmentsManager manager){

        _range = range;
        _belongsTo = belongsTo;
        _manager = manager;

        _lock = new ReentrantLock(true);
    }

    @Override
    public Range<Integer> range() {

        return _range;
    }

    @Override
    public <K, V> Cache<K, V> belongsTo() {
        return (Cache)_belongsTo;
    }

    @Override
    public <K> Serializer<K> getKeySerializer() {

        return (Serializer)_belongsTo.getKeySerializer();
    }

    @Override
    public <V> Serializer<V> getValSerializer() {

        return (Serializer)_belongsTo.getValSerializer();
    }

    @Override
    public BinStorage getStorage() {

        return _belongsTo.getStorage();
    }

    @Override
    public List<Segment> getChildren() {
        if(isLeaf()){
            return Collections.emptyList();
        }
        else{
            return Arrays.asList(_lower, _upper);
        }
    }

    @Override
    public boolean isLeaf() {

        return _lower == null || _upper == null;
    }

    @Override
    public int size() {

        try{
            _lock.lock();

            if(isLeaf()){
                return _size;
            }
            return _lower.size() + _upper.size();
        }
        finally{
            _lock.unlock();
        }
    }

    @Override
    public Segment route(final int segmentCode) {

        if(isLeaf()){
            return this;
        }

        return (segmentCode <= _lower.range().upperEndpoint() ? _lower : _upper).route(segmentCode);
    }

    public @Override <V> V get(final Get get) throws ExecutionException, IOException {

        return route(getSegmentCode(get.getHashCode())).get(get);
    }

    public @Override void put(Put put) throws IOException {

        route(getSegmentCode(put.getHashCode())).put(put);
    }


    @Override
    public boolean using(BinDocument document) {

        return route(getSegmentCode(document.getHashCode())).using(document);
    }

    public static int getSegmentCode(final int hashCode) {

        return hashCode >>> 16;//this gives us the highest 16 bits as segment code (int)
    }

    public static int getOffset(final int hashCode) {

        return MASK_OF_OFFSET & hashCode;//this gives us the lowest 15 bits as offset
    }
}
