package org.ebaysf.bluewhale.command;

import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentRaw;
import org.ebaysf.bluewhale.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Created by huzhou on 2/28/14.
 */
public class PutAsIs implements Put {

    private final Object _key;
    private final Object _val;
    private final int _hashCode;
    private final long _lastModified;

    public <K, V> PutAsIs(final K key,
                          final V val,
                          final int hashCode) {

        this(key, val, hashCode, System.nanoTime());
    }

    public <K, V> PutAsIs(final K key,
                          final V val,
                          final int hashCode,
                          final long lastModified) {

        _key = key;
        _val = val;
        _hashCode = hashCode;
        _lastModified = lastModified;
    }

    public @Override <K> K getKey(final Serializer<K> keySerializer) {
        return (K) _key;
    }

    public @Override <K> ByteBuffer getKeyAsByteBuffer(final Serializer<K> keySerializer){
        return keySerializer.serialize((K)_key);
    }

    public @Override <V> V getVal(final Serializer<V> valSerializer) {
        return (V) _val;
    }

    public @Override <V> ByteBuffer getValAsByteBuffer(final Serializer<V> valSerializer){
        return valSerializer.serialize((V)_val);
    }

    public @Override int getHashCode() {
        return _hashCode;
    }

    public @Override boolean invalidates() {
        return false;
    }

    public @Override boolean compresses(){
        return false;
    }

    public @Override boolean suppressRemovalNotification(){
        return false;
    }

    public @Override long getLastModified() {
        return _lastModified;
    }

    public @Override boolean refreshes() {
        return false;
    }

    public @Override long getNext() {
        return -1L;
    }

    @Override
    public <K, V> BinDocument create(final Serializer<K> keySerializer,
                              final Serializer<V> valSerializer,
                              final long next) {
        return new BinDocumentRaw()
                .setKey(getKeyAsByteBuffer(keySerializer))
                .setValue(getValAsByteBuffer(valSerializer))
                .setHashCode(getHashCode())
                .setNext(next)//normal token would be -1, positive tokens used only by optimizations
                .setLastModified(getLastModified())
                .setState((byte)0x00);
    }
}
