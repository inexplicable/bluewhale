package org.ebaysf.bluewhale.util;

import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * Created by huzhou on 5/6/14.
 */
public class RemovalNotificationFactory {

    public static final RemovalNotificationFactory INSTANCE = new RemovalNotificationFactory();

    private static final Logger LOGGER = LoggerFactory.getLogger(RemovalNotificationFactory.class);

    private final Constructor _constructor;

    private RemovalNotificationFactory(){

        _constructor = RemovalNotification.class.getDeclaredConstructors()[0];
        _constructor.setAccessible(true);
    }


    public <K, V> RemovalNotification<K, V> makeRemovalNotification(final K key,
                                                                    final BinDocument document,
                                                                    final Serializer<V> valSerializer,
                                                                    final RemovalCause cause) {
        try {
            return (RemovalNotification<K, V>)_constructor.newInstance(key,
                    new RemovalNotificationValueHolder(document.getValue(), document.isCompressed(), valSerializer),
                    cause);
        }
        catch(Exception ex){

            LOGGER.warn("removal notification creation failure", ex);
            return null;
        }
    }
}
