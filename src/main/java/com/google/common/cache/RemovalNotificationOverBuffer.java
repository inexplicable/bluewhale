package com.google.common.cache;

import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.serialization.Serializer;

public class RemovalNotificationOverBuffer<K, V> extends RemovalNotification<K, V> {

    private final BinDocument _doc;
    private final Serializer<V> _valSerializer;

    public RemovalNotificationOverBuffer(final K key,
                                         final BinDocument doc,
                                         final Serializer<V> serializer,
                                         final RemovalCause cause){

        super(key, null, cause);

        _doc = doc;
        _valSerializer = serializer;
    }

    public @Override V getValue(){

        return _valSerializer.deserialize(_doc.getValue(), _doc.isCompressed());
    }
}