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

        int length();

        void write(final ByteBuffer buffer, final int offset);
    }

    interface BinDocumentReader extends BinDocument {

        long SHIFTS_OF_STATE = Long.SIZE - Byte.SIZE;
        long SHIFTS_OF_KEY_LENGTH = Integer.SIZE;
        long MASK_OF_KEY_LENGTH = -1L >>> (Byte.SIZE + Integer.SIZE);
        long MASK_OF_VAL_LENGTH = -1L >>> (Integer.SIZE + 1L);

        int OFFSET_OF_NEXT = BYTES_OF_LONG;//state & lengths
        int OFFSET_OF_HASHCODE = OFFSET_OF_NEXT
                + BYTES_OF_LONG;//next token
        int OFFSET_OF_LASTMODIFIED = OFFSET_OF_HASHCODE
                + BYTES_OF_INT; //hash code
        int OFFSET_OF_KEY = OFFSET_OF_LASTMODIFIED
                + BYTES_OF_LONG;//last modified

        BinDocument read() throws IOException;

        BinDocument verify() throws IOException;
    }

    BinDocumentWriter getWriter(final BinDocument document);

    BinDocumentReader getReader(final ByteBuffer buffer, final int offset) throws IOException;

    BinDocumentReader getReader(final FileChannel fch, final int offset, final int anticipatedLength) throws IOException;

}
