package org.ebaysf.bluewhale.configurable;

import com.google.common.eventbus.EventBus;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.javatuples.Pair;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by huzhou on 3/6/14.
 */
public interface Configuration {

    File getLocal();

    <K> Serializer<K> getKeySerializer();

    <V> Serializer<V> getValSerializer();

    int getConcurrencyLevel();

    int getMaxSegmentDepth();

    int getMaxPathDepth();

    BinDocumentFactory getBinDocumentFactory();

    int getJournalLength();

    int getMaxJournals();

    int getMaxMemoryMappedJournals();

    float getLeastJournalUsageRatio();

    float getDangerousJournalsRatio();

    Pair<Long, TimeUnit> getTTL();

    boolean isCleanUpOnExit();

    EvictionStrategy getEvictionStrategy();

    EventBus getEventBus();

    ExecutorService getExecutor();
}
