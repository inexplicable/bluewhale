package org.ebaysf.bluewhale.command;

import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentRaw;
import org.ebaysf.bluewhale.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: huzhou
 * Date: 12/15/12
 * Time: 9:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class PutAsInvalidate extends PutAsIs {

    public static final ByteBuffer ZERO_BYTE_BUFFER = ByteBuffer.allocate(0);

    public <K> PutAsInvalidate(final K key,
                               final int hashCode) {

        super(key, null, hashCode);
    }

    public @Override <K, V> BinDocument create(final Serializer<K> keySerializer,
                                               final Serializer<V> valSerializer,
                                               final long next) {

        return new BinDocumentRaw()
                .setKey(getKeyAsByteBuffer(keySerializer))
                .setValue(ZERO_BYTE_BUFFER)
                .setHashCode(getHashCode())
                .setNext(next)//normal token would be -1, positive tokens used only by optimizations
                .setLastModified(getLastModified())
                .setState(BinDocument.TOMBSTONE);
    }

    public @Override boolean invalidates() {
        return true;
    }
}
