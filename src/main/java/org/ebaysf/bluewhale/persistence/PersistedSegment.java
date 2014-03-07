package org.ebaysf.bluewhale.persistence;

import com.google.common.base.Preconditions;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Range;
import org.ebaysf.bluewhale.command.Get;
import org.ebaysf.bluewhale.command.Put;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by huzhou on 3/7/14.
 */
public class PersistedSegment implements Segment {

    private final File _local;
    private final Range<Integer> _range;
    private final int _size;

    public PersistedSegment(final File local,
                            final Range<Integer> range,
                            final int size) {

        _local = Preconditions.checkNotNull(local);
        _range = Preconditions.checkNotNull(range);
        _size = size;
    }

    public @Override File local(){
        return _local;
    }

    public @Override Range<Integer> range() {
        return _range;
    }

    public @Override Configuration configuration() {
        throw new UnsupportedOperationException();
    }

    public @Override List<Segment> getChildren() {
        throw new UnsupportedOperationException();
    }

    public @Override boolean isLeaf() {
        return true;
    }

    public @Override int size() {
        return _size;
    }

    public @Override <K> Serializer<K> getKeySerializer() {
        throw new UnsupportedOperationException();
    }

    public @Override <V> Serializer<V> getValSerializer() {
        throw new UnsupportedOperationException();
    }

    public @Override BinStorage getStorage() {
        throw new UnsupportedOperationException();
    }

    public @Override Segment route(int segmentCode) {
        throw new UnsupportedOperationException();
    }

    public @Override void put(Put put) throws IOException {
        throw new UnsupportedOperationException();
    }

    public @Override <V> V get(Get get) throws ExecutionException, IOException {
        throw new UnsupportedOperationException();
    }

    public @Override boolean using(BinDocument document) {
        throw new UnsupportedOperationException();
    }

    public @Override void forget(BinDocument document, RemovalCause cause) {
        throw new UnsupportedOperationException();
    }

    public @Override void refresh(BinDocument document) {
        throw new UnsupportedOperationException();
    }
}
