package org.ebaysf.bluewhale.document;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Created by huzhou on 3/7/14.
 */
public class ByteBufferBinDocumentWithChecksumReader extends ByteBufferBinDocumentRawReader {

    public ByteBufferBinDocumentWithChecksumReader(final ByteBuffer buffer, final int offset) {

        super(buffer, offset);
    }

    public @Override BinDocument verify(){

        final ByteBuffer raw = ByteBuffer.allocate(getLength());

        int readAt = 0;
        for(int fast = _offset, bufferLimit = _buffer.limit() - BYTES_OF_LONG, readLimit = raw.limit() - BYTES_OF_LONG;
                    fast < bufferLimit && readAt < readLimit;
                    fast += BYTES_OF_LONG, readAt += BYTES_OF_LONG){

            raw.putLong(readAt, _buffer.getLong(fast));
        }

        for(int slow = _offset, bufferLimit = _buffer.limit(), readLimit = raw.limit();
                     slow < bufferLimit && readAt < readLimit;
                     slow += 1, readAt += 1){

            raw.put(_buffer.get(slow));
        }

        final CRC32 checksum = new CRC32();
        checksum.update(raw.array(), 0, raw.limit() - BYTES_OF_LONG);

        return checksum.getValue() == _buffer.getLong(_offset + raw.limit() - BYTES_OF_LONG)
                ? this
                : null;
    }

    public @Override int getLength(){

        return BinDocumentWithChecksum.getLength(_keyLength, _valLength);
    }
}
