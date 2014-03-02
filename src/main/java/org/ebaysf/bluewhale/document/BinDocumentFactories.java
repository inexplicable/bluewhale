package org.ebaysf.bluewhale.document;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by huzhou on 2/28/14.
 */
public abstract class BinDocumentFactories {

    public static final BinDocumentFactory RAW = new BinDocumentFactory() {

        @Override
        public BinDocumentWriter getWriter(BinDocument document) {
            return new BinDocumentRawWriter(document);
        }

        @Override
        public BinDocumentReader getReader(ByteBuffer buffer, int offset) throws IOException {
            return new ByteBufferBinDocumentRawReader(buffer, offset);
        }

        @Override
        public BinDocumentReader getReader(FileChannel fch, int offset, int anticipatedLength) throws IOException {
            return new FileChannelBinDocumentRawReader(fch, offset, anticipatedLength);
        }
    };
}
