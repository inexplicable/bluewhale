package org.ebaysf.bluewhale.configurable;

import com.google.common.eventbus.EventBus;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.serialization.Serializer;

import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 * Created by huzhou on 3/6/14.
 */
public interface Configuration {

    File getLocal();

    <K> Serializer<K> getKeySerializer();

    <V> Serializer<V> getValSerializer();

    int getConcurrencyLevel();

    int getMaxSegmentDepth();

    BinDocumentFactory getBinDocumentFactory();

    int getJournalLength();

    int getMaxJournals();

    int getMaxMemoryMappedJournals();

    float getLeastJournalUsageRatio();

    boolean isCleanUpOnExit();

    EventBus getEventBus();

    ExecutorService getExecutor();
}
