package org.ebaysf.bluewhale.storage;

import com.google.common.collect.Range;
import org.ebaysf.bluewhale.document.BinDocument;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by huzhou on 3/5/14.
 */
public class EvictedBinJournal implements BinJournal {

    public static final EvictedBinJournal INSTANCE = new EvictedBinJournal();

    private EvictedBinJournal(){

    }

    public @Override JournalState currentState() {
        return JournalState.Evicted;
    }

    public @Override File local() {
        throw new UnsupportedOperationException();
    }

    public @Override Range<Integer> range() {
        throw new UnsupportedOperationException();
    }

    public @Override int append(BinDocument document) throws IOException {
        throw new UnsupportedOperationException();
    }

    public @Override BinDocument read(int offset) throws IOException {
        return null;
    }

    public @Override ByteBuffer getMemoryMappedBuffer() {
        return null;
    }

    public @Override int getJournalLength() {
        return 0;
    }

    public @Override int getDocumentSize() {
        return 0;
    }

    public @Override JournalUsage usage() {
        return new JournalUsageImpl(System.nanoTime(), 0);
    }

    public @Override Iterator<BinDocument> iterator() {
        return Collections.<BinDocument>emptyList().iterator();
    }
}
