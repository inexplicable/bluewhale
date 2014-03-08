package org.ebaysf.bluewhale.document;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * Created by huzhou on 3/7/14.
 */
public class FileChannelBinDocumentWithChecksumReader extends FileChannelBinDocumentRawReader {

    public FileChannelBinDocumentWithChecksumReader(final FileChannel fch,
                                                    final int offset,
                                                    final int anticipatedLength) throws IOException {

        super(fch, offset, anticipatedLength);
    }

    public @Override BinDocument verify(){

        final byte[] raw = new byte[getLength() - BYTES_OF_LONG];
        System.arraycopy(_buffer.array(), 0, raw, 0, raw.length);

        final CRC32 checksum = new CRC32();
        checksum.update(raw);

        return checksum.getValue() == _buffer.getLong(raw.length)
                ? this
                : null;
    }

    public @Override int getLength(){

        return BinDocumentWithChecksum.getLength(_keyLength, _valLength);
    }
}
