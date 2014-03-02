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

    @Override
    public JournalState currentState() {
        return _state;
    }

    @Override
    public File local() {
        return _local;
    }

    @Override
    public Range<Integer> range() {
        return _journalRange;
    }

    @Override
    public int getJournalLength() {
        return _length;
    }

    @Override
    public int getDocumentSize() {
        return _size;
    }

    @Override
    public JournalUsage usage() {
        return _usage;
    }

}
