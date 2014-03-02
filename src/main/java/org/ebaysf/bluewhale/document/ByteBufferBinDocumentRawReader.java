package org.ebaysf.bluewhale.document;

import com.google.common.primitives.Longs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

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

        final int statesAndKeyLength = _buffer.getInt(offset);
        _state = (byte)(statesAndKeyLength >>> 24);
        _keyLength = (-1 >>> 8) & statesAndKeyLength;
        _valLength = _buffer.getInt(offset + 4);
    }

    public @Override ByteBuffer getKey() {
        final ByteBuffer keyBuffer = _buffer.duplicate();
        final int pos = _offset + 20;
        keyBuffer.position(pos).limit(pos + _keyLength);
        return keyBuffer;
    }

    public @Override ByteBuffer getValue() {
        final ByteBuffer valBuffer = _buffer.duplicate();
        final int pos = _offset + 20 + _keyLength;
        valBuffer.position(pos).limit(pos + _valLength);
        return valBuffer;
    }

    public @Override int getHashCode() {
        return _buffer.getInt(_offset + 16);
    }

    public @Override long getNext() {
        return _buffer.getLong(_offset + 8);
    }

    public @Override long getLastModified() {
        return _buffer.getLong(_offset + 20 + _keyLength + _valLength);
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
        final ByteBuffer raw = readRaw();
        if (raw == null){
            return null;
        }

        final CRC32 checksum = new CRC32();
        checksum.update(raw.array(), 0, raw.limit() - Longs.BYTES);

        return checksum.getValue() == _buffer.getLong(_offset + raw.limit() - Longs.BYTES)
                ? new ByteBufferBinDocumentRawReader(raw.duplicate(), 0)
                : null;
    }

    protected ByteBuffer readRaw() {
        final ByteBuffer raw = ByteBuffer.allocate(BinDocumentRaw.getLength(_keyLength, _valLength));

        int readAt = 0;
        for(int fast = _offset, bufferLimit = _buffer.limit() - Longs.BYTES, readLimit = raw.limit() - Longs.BYTES;
            fast < bufferLimit && readAt < readLimit;
            fast += Longs.BYTES, readAt += Longs.BYTES){

            raw.putLong(readAt, _buffer.getLong(fast));
        }
        for(int slow = _offset, bufferLimit = _buffer.limit(), readLimit = raw.limit();
            slow < bufferLimit && readAt < readLimit;
            slow += 1, readAt += 1){

            raw.put(_buffer.get(slow));
        }

        if(readAt < raw.limit()){
            return null;
        }
        return raw;
    }

    public @Override int getLength(){
        return BinDocumentRaw.getLength(_keyLength, _valLength);
    }
}
