package org.ebaysf.bluewhale.document;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

/**
 * the layout of the BinDocumentRaw is as the following:
 *
 * 1. 3 bytes int <=> length of key
 * 1. 1 byte state <=> states of doc, whether tombstone, compressed, value loading etc. it's united with the length of key
 * -- therefore the largest length of key is 2 ^ 24 - 1 apprx 16mb, it's safe to exclude such use case for now as the key is really "too large to be a key"
 * 2. 4 bytes int <=> length of value
 * 3. 8 bytes long <=> next
 * 4. 4 bytes int <=> hashCode
 * 5. 8 bytes long <=> last modified time
 * 6. byte[length of key] <=> key bytes
 * 7. byte[length of value] <=> value bytes
 * 8. 8 bytes long <=> CRC32 checksum
 */
public class BinDocumentWithChecksumWriter extends BinDocumentRawWriter {

    public BinDocumentWithChecksumWriter(final BinDocument doc) {

        super(doc);
    }

    public @Override int getLength() {

        return BinDocumentWithChecksum.getLength(getKey().remaining(), getValue().remaining());
    }

    public @Override void write(final ByteBuffer buffer,
                                final int offset) {

        final ByteBuffer key = getKey();
        final ByteBuffer val = getValue();

        Preconditions.checkState(key.remaining() < MAX_KEY_LENGTH);

        final ByteBuffer writer = buffer.duplicate();

        writer.position(offset);
        writer.putLong((((long)(getState() << STATE_SHIFTS | key.remaining())) << STATE_AND_KEY_LENGTH_SHIFTS) | (long)val.remaining());
        writer.putLong(getNext());
        writer.putInt(getHashCode());
        writer.putLong(getLastModified());
        writer.put(key);
        writer.put(val);
        //additional checksum to complete the write.
        writer.putLong(BinDocumentWithChecksum.getChecksum(buffer, offset, getLength() - BYTES_OF_LONG));
    }
}
