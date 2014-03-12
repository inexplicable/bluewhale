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

    /**
     * @return key as ByteBuffer
     */
    ByteBuffer getKey();

    /**
     * @return value as ByteBuffer
     */
    ByteBuffer getValue();

    /**
     * @return hashCode of key, @see Serializer#hashCode
     */
    int getHashCode();

    /**
     * @return next token linking to the previous document in the hash slot
     */
    long getNext();

    /**
     * @return last modified time of the document
     */
    long getLastModified();

    /**
     * assert if the state is assigned, could be TOMBSTONE, VALUE_LOADING, COMPRESSED etc.
     * @param state
     * @return
     */
    byte getState();

    /**
     * @return boolean whether the document was created by @see PutAsInvalidate
     */
    boolean isTombstone();

    /**
     * @return boolean whether the document value has been compressed using snappy
     */
    boolean isCompressed();

    /**
     * @return number of bytes
     */
    int getLength();
}
