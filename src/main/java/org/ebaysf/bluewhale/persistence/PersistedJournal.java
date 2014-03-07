package org.ebaysf.bluewhale.persistence;

import com.google.common.collect.Range;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.ebaysf.bluewhale.storage.JournalUsage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by huzhou on 3/7/14.
 */
public class PersistedJournal implements BinJournal {

    private final File _local;
    private final JournalState _state;
    private final Range<Integer> _journalRange;
    private final int _length;
    private final int _size;
    private final JournalUsage _journalUsage;

    public PersistedJournal(final File local,
                            final JournalState state,
                            final Range<Integer> range,
                            final JournalUsage usage,
                            final int length,
                            final int size) {

        _local = local;
        _state = state;
        _journalRange = range;
        _journalUsage = usage;
        _length = length;
        _size = size;
    }

    public @Override JournalState currentState() {
        return _state;
    }

    public @Override File local() {
        return _local;
    }

    public @Override Range<Integer> range() {
        return _journalRange;
    }

    public @Override int getJournalLength() {
        return _length;
    }

    public @Override int getDocumentSize() {
        return _size;
    }


    public @Override JournalUsage usage() {
        return _journalUsage;
    }

    public @Override int append(BinDocument document) throws IOException {
        throw new UnsupportedOperationException();
    }

    public @Override BinDocument read(int offset) throws IOException {
        throw new UnsupportedOperationException();
    }

    public @Override ByteBuffer getMemoryMappedBuffer() {
        throw new UnsupportedOperationException();
    }

    public @Override Iterator<BinDocument> iterator() {
        throw new UnsupportedOperationException();
    }
}
