package org.ebaysf.bluewhale.document;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by huzhou on 2/28/14.
 */
public abstract class BinDocumentFactories {

    public static final BinDocumentFactory RAW = new BinDocumentFactory() {

        public @Override BinDocumentWriter getWriter(final BinDocument document) {

            return new BinDocumentRawWriter(document);
        }

        public @Override BinDocumentReader getReader(final ByteBuffer buffer,
                                                     final int offset) throws IOException {

            return new ByteBufferBinDocumentRawReader(buffer, offset);
        }

        public @Override BinDocumentReader getReader(final FileChannel fch,
                                                     final int offset,
                                                     final int anticipatedLength) throws IOException {

            return new FileChannelBinDocumentRawReader(fch, offset, anticipatedLength);
        }
    };

    public static final BinDocumentFactory CHECKED = new BinDocumentFactory() {

        public @Override BinDocumentWriter getWriter(final BinDocument document) {

            return new BinDocumentWithChecksumWriter(document);
        }

        public @Override BinDocumentReader getReader(final ByteBuffer buffer,
                                                     final int offset) throws IOException {

            return new ByteBufferBinDocumentWithChecksumReader(buffer, offset);
        }

        public @Override BinDocumentReader getReader(final FileChannel fch,
                                                     final int offset,
                                                     final int anticipatedLength) throws IOException {

            return new FileChannelBinDocumentWithChecksumReader(fch, offset, anticipatedLength);
        }
    };
}
