package org.ebaysf.bluewhale.command;

import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Created by huzhou on 2/26/14.
 */
public interface Put {

    <K> K getKey(final Serializer<K> keySerializer);

    <V> V getVal(final Serializer<V> valSerializer);

    <K> ByteBuffer getKeyAsByteBuffer(final Serializer<K> keySerializer);

    <V> ByteBuffer getValAsByteBuffer(final Serializer<V> valSerializer);

    <K, V> BinDocument create(final Serializer<K> keySerializer, final Serializer<V> valSerializer, final long next);

    int getHashCode();

    long getNext();

    long getLastModified();

    boolean invalidates();

    boolean refreshes();

    boolean compresses();

    boolean suppressRemovalNotification();
}
