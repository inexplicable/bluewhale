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
        _alives = new SparseBitSet(Math.max(documents, 1024));
    }

    public @Override long getLastModified() {
        return _lastModified;
    }

    public @Override int getDocuments(){
        return _documents;
    }

    public @Override boolean isAllDead() {

        return _alives.isEmpty();
    }

    public @Override boolean isUsageRatioAbove(final float usageRatioAtLeast){

        return _alives.cardinality() >= _documents * usageRatioAtLeast;
    }

    public @Override SparseBitSet getAlives() {
        return _alives;
    }
}
