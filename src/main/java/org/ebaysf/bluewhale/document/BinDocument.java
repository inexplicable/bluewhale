package org.ebaysf.bluewhale.document;

import java.nio.ByteBuffer;

/**
 * Created by huzhou on 2/26/14.
 */
public interface BinDocument {

    int BYTES_OF_SHORT = Short.SIZE / Byte.SIZE;
    int BYTES_OF_INT = Integer.SIZE / Byte.SIZE;
    int BYTES_OF_LONG = Long.SIZE / Byte.SIZE;

    byte TOMBSTONE = 0x01;
    byte VALUE_LOADING = 0x02;
    byte COMPRESSED = 0x04;

    ByteBuffer getKey();

    ByteBuffer getValue();

    int getHashCode();

    long getNext();

    long getLastModified();

    /**
     * assert if the state is assigned, could be TOMBSTONE, VALUE_LOADING, COMPRESSED etc.
     * @param state
     * @return
     */
    byte getState();

    boolean isTombstone();

    boolean isCompressed();

    /**
     * @return number of bytes
     */
    int getLength();
}
