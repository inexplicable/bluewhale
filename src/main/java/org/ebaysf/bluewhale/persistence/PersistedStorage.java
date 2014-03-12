package org.ebaysf.bluewhale.persistence;

import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.ebaysf.bluewhale.storage.UsageTrack;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by huzhou on 3/7/14.
 */
public class PersistedStorage implements BinStorage {

    private final List<BinJournal> _navigableJournals;

    public PersistedStorage(final List<BinJournal> journals){
        
        _navigableJournals = journals;
    }

    public @Override File local() {
        throw new UnsupportedOperationException();
    }

    public @Override long append(BinDocument binDocument) throws IOException {
        throw new UnsupportedOperationException();
    }

    public @Override BinDocument read(long token) throws IOException {
        throw new UnsupportedOperationException();
    }

    public @Override BinJournal route(long token) {
        throw new UnsupportedOperationException();
    }

    public @Override boolean isDangerous(long token) {
        throw new UnsupportedOperationException();
    }

    public @Override UsageTrack getUsageTrack() {
        throw new UnsupportedOperationException();
    }

    public @Override int getJournalLength() {
        throw new UnsupportedOperationException();
    }

    public @Override int getMaxJournals() {
        throw new UnsupportedOperationException();
    }

    public @Override int getMaxMemoryMappedJournals() {
        throw new UnsupportedOperationException();
    }

    public @Override Iterator<BinJournal> iterator() {
        return _navigableJournals.iterator();
    }
}
