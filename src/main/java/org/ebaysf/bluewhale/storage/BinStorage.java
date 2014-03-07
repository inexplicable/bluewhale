package org.ebaysf.bluewhale.storage;

import org.ebaysf.bluewhale.document.BinDocument;

import java.io.File;
import java.io.IOException;

/**
 * Created by huzhou on 2/26/14.
 */
public interface BinStorage extends Iterable<BinJournal> {


    enum InspectionReport {
        EvictionRequiredBySize,
        EvictionRequiredByTTL,
        CompressionRequired,
        RemainAsIs
    }

    /**
     * where the journals are stored, could be temporary folders
     * @return
     */
    File local();

    /**
     * append a binary document to the writable journal
     * @param binDocument
     * @return
     * @throws IOException
     */
    long append(final BinDocument binDocument) throws IOException;

    /**
     * find the binary document from the journals
     * @param token
     * @return
     * @throws IOException
     */
    BinDocument read(final long token) throws IOException;

    /**
     * find the journal using token
     * @param token
     * @return
     */
    BinJournal route(final long token);

    boolean isDangerous(final long token);

    UsageTrack getUsageTrack();

    int getJournalLength();

    int getMaxJournals();

    int getMaxMemoryMappedJournals();

    int getEvictedJournals();

}
