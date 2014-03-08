package org.ebaysf.bluewhale.segment;

import com.google.common.base.Preconditions;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Range;
import org.ebaysf.bluewhale.command.Get;
import org.ebaysf.bluewhale.command.Put;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;

import java.io.File;
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

    private final File _local;
    private final Range<Integer> _range;

    protected final transient Configuration _configuration;
    protected final transient SegmentsManager _manager;
    protected final transient BinStorage _storage;
    protected final transient ReentrantLock _lock;

    protected volatile int _size;
    protected volatile transient Segment _lower;
    protected volatile transient Segment _upper;

    public AbstractSegment(final File local,
                           final Range<Integer> range,
                           final Configuration configuration,
                           final SegmentsManager manager,
                           final BinStorage storage,
                           final int size){

        _local = Preconditions.checkNotNull(local);
        _range = Preconditions.checkNotNull(range);
        _configuration = Preconditions.checkNotNull(configuration);
        _manager = Preconditions.checkNotNull(manager);
        _storage = Preconditions.checkNotNull(storage);
        _size = size;Preconditions.checkArgument(size >= 0);

        _lock = new ReentrantLock(true);
    }

    public @Override File local(){
        return _local;
    }

    public @Override Range<Integer> range() {

        return _range;
    }

    public @Override Configuration configuration() {
        return _configuration;
    }

    public @Override <K> Serializer<K> getKeySerializer() {

        return (Serializer<K>)_configuration.getKeySerializer();
    }

    public @Override <V> Serializer<V> getValSerializer() {

        return (Serializer<V>)_configuration.getValSerializer();
    }

    public @Override BinStorage getStorage() {

        return _storage;
    }

    public @Override List<Segment> getChildren() {
        if(isLeaf()){
            return Collections.emptyList();
        }
        else{
            return Arrays.asList(_lower, _upper);
        }
    }

    public @Override boolean isLeaf() {

        return _lower == null || _upper == null;
    }

    public @Override int size() {

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

    public @Override Segment route(final int segmentCode) {

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


    public @Override boolean using(final BinDocument document) {

        return route(getSegmentCode(document.getHashCode())).using(document);
    }

    public @Override void forget(final BinDocument document, final RemovalCause cause) {

        route(getSegmentCode(document.getHashCode())).forget(document, cause);
    }

    public @Override void refresh(final BinDocument document){

        route(getSegmentCode(document.getHashCode())).refresh(document);
    }

    public @Override String toString(){
        return new StringBuilder()
                .append("segment=")
                .append(_range)
                .append("&leaf=")
                .append(isLeaf())
                .append("&size=")
                .append(size()).toString();
    }

    public static int getSegmentCode(final int hashCode) {

        return hashCode >>> 16;//this gives us the highest 16 bits as segment code (int)
    }

    public static int getOffset(final int hashCode) {

        return MASK_OF_OFFSET & hashCode;//this gives us the lowest 15 bits as offset
    }
}
