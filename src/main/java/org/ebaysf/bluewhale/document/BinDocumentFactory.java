package org.ebaysf.bluewhale.document;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created with IntelliJ IDEA.
 * User: huzhou
 * Date: 12/13/12
 * Time: 8:19 PM
 * To change this template use File | Settings | File Templates.
 */
public interface BinDocumentFactory {

    interface BinDocumentWriter extends BinDocument {

        ByteBuffer write() throws IOException;

        int length();

        void write(final ByteBuffer buffer, final int offset);
    }

    interface BinDocumentReader extends BinDocument {

        BinDocument read() throws IOException;

        BinDocument verify() throws IOException;
    }

    BinDocumentWriter getWriter(final BinDocument document);

    BinDocumentReader getReader(final ByteBuffer buffer, final int offset) throws IOException ;

    BinDocumentReader getReader(final FileChannel fch, final int offset, final int anticipatedLength) throws IOException;

}
