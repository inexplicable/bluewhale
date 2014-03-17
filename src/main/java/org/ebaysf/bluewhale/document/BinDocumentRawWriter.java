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
 * 7. byte[length of key] <=> key bytes
 * 8. byte[length of value] <=> value bytes
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

    protected static final int MAX_KEY_LENGTH = 1 << (Integer.SIZE - Byte.SIZE);
    protected static final int STATE_SHIFTS = Integer.SIZE - Byte.SIZE;
    protected static final int STATE_AND_KEY_LENGTH_SHIFTS = Integer.SIZE;

    public @Override void write(final ByteBuffer buffer, final int offset) {

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
    }
}
