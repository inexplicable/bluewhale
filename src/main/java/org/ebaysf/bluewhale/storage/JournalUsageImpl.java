package org.ebaysf.bluewhale.storage;

import org.brettw.SparseBitSet;

/**
 * Created by huzhou on 3/2/14.
 */
public class JournalUsageImpl implements JournalUsage {

    private final long _lastModified;
    private final int _documents;
    private final SparseBitSet _alives;

    public JournalUsageImpl(final long lastModified,
                            final int documents){

        this(lastModified, documents, null);
    }

    public JournalUsageImpl(final long lastModified,
                            final int documents,
                            final SparseBitSet alives){

        _lastModified = lastModified;
        _documents = documents;

        if(alives != null){
            _alives = new SparseBitSet(alives.length());
            _alives.or(alives);
        }
        else {
            _alives = new SparseBitSet(documents);
        }
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
