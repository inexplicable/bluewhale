package org.ebaysf.bluewhale.document;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reader that knows how to read from raw bytes
 */
public class ByteBufferBinDocumentRawReader implements BinDocumentFactory.BinDocumentReader {

    private final ByteBuffer _buffer;
    private final int _offset;
    private final byte _state;
    private final int _keyLength;
    private final int _valLength;
    
    public ByteBufferBinDocumentRawReader(final ByteBuffer buffer, final int offset){
        _buffer = buffer.duplicate();
        _offset = offset;

        final long statesAndLengths = _buffer.getLong(offset);
        _state = (byte)(statesAndLengths >>> SHIFTS_OF_STATE);
        _keyLength = (int)(MASK_OF_KEY_LENGTH & (statesAndLengths >>> SHIFTS_OF_KEY_LENGTH));
        _valLength = (int)(MASK_OF_VAL_LENGTH & statesAndLengths);
    }

    public @Override ByteBuffer getKey() {
        final ByteBuffer keyBuffer = _buffer.duplicate();
        final int pos = _offset + OFFSET_OF_KEY;
        keyBuffer.position(pos).limit(pos + _keyLength);
        return keyBuffer;
    }

    public @Override ByteBuffer getValue() {
        final ByteBuffer valBuffer = _buffer.duplicate();
        final int pos = _offset + OFFSET_OF_KEY + _keyLength;
        valBuffer.position(pos).limit(pos + _valLength);
        return valBuffer;
    }

    public @Override int getHashCode() {
        return _buffer.getInt(_offset + OFFSET_OF_HASHCODE);
    }

    public @Override long getNext() {
        return _buffer.getLong(_offset + OFFSET_OF_NEXT);
    }

    public @Override long getLastModified() {
        return _buffer.getLong(_offset + OFFSET_OF_LASTMODIFIED);
    }

    public @Override byte getState() {
        return _state;
    }
    
    public @Override BinDocument read() throws IOException {
        return this;
    }

    public @Override boolean isTombstone(){
        return (_state & TOMBSTONE) != 0;
    }

    public @Override boolean isCompressed(){
        return (_state & COMPRESSED) != 0;
    }

    public @Override BinDocument verify(){
        return this;
    }

    public @Override int getLength(){
        return BinDocumentRaw.getLength(_keyLength, _valLength);
    }
}
