package org.ebaysf.bluewhale.storage;

import org.brettw.SparseBitSet;

/**
 * Created by huzhou on 2/27/14.
 */
public interface JournalUsage {

    long getLastModified();

    boolean isAllDead();

    boolean isUsageRatioAbove(final float usageRatioAtLeast);

    SparseBitSet getAlives();
}
