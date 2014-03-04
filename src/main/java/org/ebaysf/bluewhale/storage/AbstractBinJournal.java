package org.ebaysf.bluewhale.storage;

import com.google.common.collect.Range;
import org.ebaysf.bluewhale.document.BinDocumentFactory;

import java.io.File;

/**
 * Created by huzhou on 2/27/14.
 */
public abstract class AbstractBinJournal implements BinJournal {

    private final File _local;
    private final JournalState _state;
    private final Range<Integer> _journalRange;
    private final int _length;

    protected final JournalsManager _manager;
    protected final BinDocumentFactory _factory;
    protected volatile int _size;
    protected volatile JournalUsage _usage;

    public AbstractBinJournal(final File local,
                              final JournalState state,
                              final Range<Integer> range,
                              final JournalsManager manager,
                              final BinDocumentFactory factory,
                              final int length){
        _local = local;
        _state = state;
        _journalRange = range;
        _length = length;

        _manager = manager;
        _factory = factory;
        _size = 0;
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
        return _usage;
    }

    public @Override String toString(){
        return new StringBuilder()
                .append("[journal]")
                .append(_journalRange)
                .append("][size:")
                .append(getDocumentSize())
                .append("]").toString();
    }

}
