package org.ebaysf.bluewhale.document;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created with IntelliJ IDEA.
 * User: huzhou
 * Date: 12/13/12
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileChannelBinDocumentRawReader implements BinDocumentFactory.BinDocumentReader {

    private final FileChannel _fch;
    private final byte _state;
    private final int _keyLength;
    private final int _valLength;
    private final ByteBuffer _buffer;

    public FileChannelBinDocumentRawReader(final FileChannel fch,
                                           final int offset,
                                           final int anticipatedLength) throws IOException {

        _fch = fch;

        final ByteBuffer buffer = ByteBuffer.allocate(anticipatedLength);
        _fch.read(buffer, offset);

        final long statesAndLengths = buffer.getLong(0);
        _state = (byte)(statesAndLengths >>> SHIFTS_OF_STATE);
        _keyLength = (int)(MASK_OF_KEY_LENGTH & (statesAndLengths >>> SHIFTS_OF_KEY_LENGTH));
        _valLength = (int)(MASK_OF_VAL_LENGTH & statesAndLengths);

        final int actualLength = getLength();
        if(actualLength > anticipatedLength){
            final ByteBuffer completeBuffer = ByteBuffer.allocate(actualLength);
            completeBuffer.put(buffer);
            _fch.read(completeBuffer, offset + anticipatedLength);
            _buffer = completeBuffer;
        }
        else{
            _buffer = buffer;
        }
    }

    public @Override ByteBuffer getKey() {
        final ByteBuffer keyBuffer = _buffer.duplicate();
        keyBuffer.position(OFFSET_OF_KEY).limit(OFFSET_OF_KEY + _keyLength);
        return keyBuffer;
    }

    public @Override ByteBuffer getValue() {
        final ByteBuffer valBuffer = _buffer.duplicate();
        final int pos = OFFSET_OF_KEY + _keyLength;
        valBuffer.position(pos).limit(pos + _valLength);
        return valBuffer;
    }

    public @Override int getHashCode() {
        return _buffer.getInt(OFFSET_OF_HASHCODE);
    }

    public @Override long getNext() {
        return _buffer.getLong(OFFSET_OF_NEXT);
    }

    public @Override long getLastModified() {
        return _buffer.getLong(OFFSET_OF_LASTMODIFIED);
    }

    public @Override byte getState() {
        return _state;
    }

    public @Override boolean isTombstone(){
        return (_state & TOMBSTONE) != 0;
    }

    public @Override boolean isCompressed(){
        return (_state & COMPRESSED) != 0;
    }

    public @Override BinDocument read(){
        return this;
    }

    public @Override BinDocument verify(){
        return this;
    }

    public @Override int getLength(){
        return BinDocumentRaw.getLength(_keyLength, _valLength);
    }
}
