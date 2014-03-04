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
 * 5. byte[length of key] <=> key bytes
 * 6. byte[length of value] <=> value bytes
 * 7. 8 bytes long <=> last modified time
 * 8. 8 bytes long <=> CRC32 checksum
 */
public class BinDocumentRawWriter implements BinDocumentFactory.BinDocumentWriter {

    private final BinDocument _doc;

    public BinDocumentRawWriter(final BinDocument doc) {
        _doc = doc;
    }

    public @Override ByteBuffer getKey() {
        return _doc.getKey();
    }

    public @Override ByteBuffer getValue() {
        return _doc.getValue();
    }

    public @Override int getHashCode() {
        return _doc.getHashCode();
    }

    public @Override long getNext() {
        return _doc.getNext();
    }

    public @Override long getLastModified() {
        return _doc.getLastModified();
    }

    public @Override byte getState() {
        return _doc.getState();
    }

    public @Override int getLength() {
        return BinDocumentRaw.getLength(getKey().remaining(), getValue().remaining());
    }

    public @Override boolean isTombstone(){
        return _doc.isTombstone();
    }

    public @Override boolean isCompressed(){
        return _doc.isCompressed();
    }

    public @Override int length() {

        return BinDocumentRaw.getLength(getKey().remaining(), getValue().remaining());
    }

    public @Override void write(final ByteBuffer buffer, final int offset) {

        final ByteBuffer key = getKey();
        Preconditions.checkState(key.remaining() < (1 << 24) - 1);

        final ByteBuffer val = getValue();

        final ByteBuffer writer = buffer.duplicate();

        writer.position(offset);
        writer.putLong((long)(getState()) << (24L + 32L) | (long)(key.remaining()) << 32L | (long)val.remaining());
        writer.putLong(getNext());
        writer.putInt(getHashCode());
        writer.putLong(getLastModified());
        writer.put(key);
        writer.put(val);
    }
}
