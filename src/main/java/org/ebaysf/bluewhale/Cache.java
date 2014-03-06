package org.ebaysf.bluewhale;

import com.google.common.eventbus.EventBus;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;

import java.util.concurrent.ExecutorService;

/**
 * Created by huzhou on 2/26/14.
 */
public interface Cache<K, V> extends com.google.common.cache.Cache<K, V> {

    EventBus getEventBus();

    ExecutorService getExecutor();

    Serializer<K> getKeySerializer();

    Serializer<V> getValSerializer();

    BinStorage getStorage();
}
