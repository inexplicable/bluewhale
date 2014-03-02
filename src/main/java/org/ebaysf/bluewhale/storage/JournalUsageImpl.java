package org.ebaysf.bluewhale.storage;

import org.brettw.SparseBitSet;

/**
 * Created by huzhou on 3/2/14.
 */
public class JournalUsageImpl implements JournalUsage {

    private final long _lastModified;
    private final int _documents;
    private final SparseBitSet _alives;

    public JournalUsageImpl(final long lastModified, final int documents){

        _lastModified = lastModified;
        _documents = documents;
        _alives = new SparseBitSet(documents);
    }

    @Override
    public long getLastModified() {
        return _lastModified;
    }

    @Override
    public boolean isAllDead() {
        return _alives.isEmpty();
    }

    @Override
    public float getUsageRatio() {
        return (float)_alives.cardinality() / (float)_documents ;
    }

    @Override
    public SparseBitSet getAlives() {
        return _alives;
    }
}
