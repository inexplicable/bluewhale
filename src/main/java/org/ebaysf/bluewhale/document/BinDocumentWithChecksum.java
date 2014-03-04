package org.ebaysf.bluewhale.document;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Created by huzhou on 3/3/14.
 */
public class BinDocumentWithChecksum extends BinDocumentRaw {

    public static int getLength(final int keyLength, final int valLength) {

        return BYTES_OF_INT //length of `key` buffer, highest byte as for `state`
                + BYTES_OF_INT //length of `value' buffer
                + BYTES_OF_LONG //next token
                + BYTES_OF_INT  //hashCode
                + BYTES_OF_LONG //last modified
                + keyLength //bytes of `key`
                + valLength //bytes of `value`
                + BYTES_OF_LONG;//checksum
    }

    public static long getChecksum(final ByteBuffer buffer, final int offset, final int length) {

        final CRC32 crc32 = new CRC32();
        if(buffer.hasArray()){
            crc32.update(buffer.array(), offset, length);
        }
        else{
            final byte[] copy = new byte[length];
            buffer.get(copy, offset, length);
            crc32.update(copy, offset, length);
        }
        return crc32.getValue();
    }
}
