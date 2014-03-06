package org.ebaysf.bluewhale;

import org.ebaysf.bluewhale.serialization.Serializer;

/**
 * Created by huzhou on 2/26/14.
 */
public interface Cache<K, V> extends com.google.common.cache.Cache<K, V> {

    Serializer<K> getKeySerializer();

    Serializer<V> getValSerializer();

}
