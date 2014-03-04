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
    private ByteBuffer _buffer;

    public FileChannelBinDocumentRawReader(final FileChannel fch, final int offset, final int anticipatedLength)
        throws IOException {

        _fch = fch;
        _buffer = ByteBuffer.allocate(anticipatedLength);
        _fch.read(_buffer, offset);

        final long statesAndLengths = _buffer.getLong(offset);
        _state = (byte)(statesAndLengths >>> SHIFTS_OF_STATE);
        _keyLength = (int)(MASK_OF_KEY_LENGTH & (statesAndLengths >>> SHIFTS_OF_KEY_LENGTH));
        _valLength = (int)(MASK_OF_VAL_LENGTH & statesAndLengths);

        final int actualLength = getLength();
        if(actualLength > anticipatedLength){
            final ByteBuffer actualBuffer = ByteBuffer.allocate(actualLength);
            actualBuffer.put(_buffer);
            _fch.read(actualBuffer, offset + anticipatedLength);
            _buffer = actualBuffer;
        }
    }

    public @Override ByteBuffer getKey() {
        final ByteBuffer keyBuffer = _buffer.duplicate();
        keyBuffer.position(20).limit(20 + _keyLength);
        return keyBuffer;
    }

    public @Override ByteBuffer getValue() {
        final ByteBuffer valBuffer = _buffer.duplicate();
        final int pos = 20 + _keyLength;
        valBuffer.position(pos).limit(pos + _valLength);
        return valBuffer;
    }

    public @Override int getHashCode() {
        return _buffer.getInt(16);
    }

    public @Override long getNext() {
        return _buffer.getLong(8);
    }

    public @Override long getLastModified() {
        return _buffer.getLong(20 + _keyLength + _valLength);
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
