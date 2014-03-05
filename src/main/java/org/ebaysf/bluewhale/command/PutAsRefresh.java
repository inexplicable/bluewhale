package org.ebaysf.bluewhale.command;

import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentRaw;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.xerial.snappy.Snappy;

import java.nio.ByteBuffer;

/**
 * Created by huzhou on 3/4/14.
 */
public class PutAsRefresh implements Put {

    private final ByteBuffer _keyAsByteBuffer;
    private ByteBuffer _valAsByteBuffer;
    private final int _hashCode;
    private final long _lastModified;
    protected byte _state;

    public PutAsRefresh(final ByteBuffer keyAsByteBuffer,
                        final ByteBuffer valAsByteBuffer,
                        final int hashCode,
                        final byte state) {

        _keyAsByteBuffer = keyAsByteBuffer.duplicate();

        _hashCode = hashCode;
        _lastModified = System.nanoTime();

        if((state & BinDocument.COMPRESSED) == 0){
            final ByteBuffer compressing = ByteBuffer.allocate(valAsByteBuffer.remaining());
            try {
                final int length = Snappy.compress(valAsByteBuffer, compressing);
                compressing.rewind();
                compressing.limit(length);
                _valAsByteBuffer = compressing;
                _state = (byte)(state & BinDocument.COMPRESSED);
            }
            catch(Exception e) {
                _valAsByteBuffer = valAsByteBuffer;
            }
        }
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

    public @Override boolean suppressRemovalNotification() {
        return true;
    }
}
