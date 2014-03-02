package org.ebaysf.bluewhale.storage;

import org.ebaysf.bluewhale.document.BinDocument;

/**
 * Created by huzhou on 2/26/14.
 * This is mainly to decouple segment dependencies from journal
 *
 */
public interface UsageTrack {

    /**
     * tells whether the binary document is still in use
     * @param document
     * @return
     */
    boolean using(final BinDocument document);
}
