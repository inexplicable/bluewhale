package org.ebaysf.bluewhale.command;

import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentRaw;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.serialization.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by huzhou on 3/4/14.
 */
public class PutAsRefresh implements Put {

    private static final Logger LOG = LoggerFactory.getLogger(PutAsRefresh.class);
    private static final byte COMPRESSED_OR_TOMBSTONE = BinDocument.COMPRESSED | BinDocument.TOMBSTONE;

    protected final byte _state;

    private final ByteBuffer _keyAsByteBuffer;
    private final ByteBuffer _valAsByteBuffer;
    private final int _hashCode;
    private final long _lastModified;

    public PutAsRefresh(final ByteBuffer keyAsByteBuffer,
                        final ByteBuffer valAsByteBuffer,
                        final int hashCode,
                        final byte state) {

        _keyAsByteBuffer = keyAsByteBuffer.duplicate();

        _hashCode = hashCode;
        _lastModified = System.nanoTime();

        ByteBuffer pendingVal = null;
        byte pendingState = 0x00;

        if((state & COMPRESSED_OR_TOMBSTONE) == 0){ //yet compressed, not tombstone
            try {
                final ByteBuffer compression = Serializers.compress(valAsByteBuffer);
                pendingState = (byte)(state | BinDocument.COMPRESSED);
                pendingVal = compression;
            }
            catch(Exception e) {
                LOG.error("compression failed", e);
                pendingState = state;
                pendingVal = valAsByteBuffer.duplicate();
            }
        }
        else{
            pendingState = state;
            pendingVal = valAsByteBuffer.duplicate();
        }

        //already compressed, or is a tombstone, or compression failed;
        _state = pendingState;
        _valAsByteBuffer = pendingVal;
    }

    public @Override <K> K getKey(Serializer<K> keySerializer) {
        return keySerializer.deserialize(_keyAsByteBuffer, false);
    }

    public @Override <V> V getVal(Serializer<V> valSerializer) {
        return valSerializer.deserialize(_valAsByteBuffer, compresses());
    }

    public @Override <K> ByteBuffer getKeyAsByteBuffer(Serializer<K> keySerializer) {
        return _keyAsByteBuffer.duplicate();
    }

    public @Override <V> ByteBuffer getValAsByteBuffer(Serializer<V> valSerializer) {
        return _valAsByteBuffer.duplicate();
    }

    public @Override <K, V> BinDocument create(final Serializer<K> keySerializer,
                                     final Serializer<V> valSerializer,
                                     final long next) {

        return new BinDocumentRaw()
                .setKey(getKeyAsByteBuffer(keySerializer))
                .setValue(getValAsByteBuffer(valSerializer))
                .setHashCode(getHashCode())
                .setNext(next)//normal token would be -1, positive tokens used only by optimizations
                .setLastModified(getLastModified())
                .setState(_state);
    }

    public @Override int getHashCode() {
        return _hashCode;
    }

    public @Override long getNext() {
        return -1L;
    }

    public @Override long getLastModified() {
        return _lastModified;
    }

    public @Override boolean invalidates() {
        return (_state & BinDocument.TOMBSTONE) != 0;
    }

    public @Override boolean refreshes() {
        return true;
    }

    public @Override boolean compresses() {
        return (_state & BinDocument.COMPRESSED) != 0;
    }
}
