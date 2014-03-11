package org.ebaysf.bluewhale.storage;

import com.google.common.collect.Range;
import org.ebaysf.bluewhale.document.BinDocument;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by huzhou on 2/26/14.
 */
public interface BinJournal extends Iterable<BinDocument> {

    enum JournalState {
        Evicted,
        FileChannelReadOnly,
        BufferedReadOnly,
        BufferedWritable;

        public boolean isWritable(){
            return this == BufferedWritable;
        }

        public boolean isEvicted(){
            return this == Evicted;
        }

        public boolean isMemoryMapped(){
            return this == BufferedReadOnly || this == BufferedWritable;
        }
    }

    int NEVER_GOING_TO_HAPPEN = Integer.MIN_VALUE;
    int INSUFFICIENT_JOURNAL_SPACE = -1;

    JournalState currentState();

    File local();

    Range<Integer> range();

    int append(final BinDocument document) throws IOException;

    BinDocument read(final int offset) throws IOException;

    ByteBuffer getMemoryMappedBuffer();

    int getJournalLength();

    int getDocumentSize();

    JournalUsage usage();
}
