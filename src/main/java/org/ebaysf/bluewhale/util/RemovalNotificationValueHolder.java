package org.ebaysf.bluewhale.util;

import org.ebaysf.bluewhale.serialization.Serializer;
import java.nio.ByteBuffer;

/**
 * Created by huzhou on 5/6/14.
 */
public class RemovalNotificationValueHolder {

    private final ByteBuffer _value;
    private final boolean _compressed;
    private final Serializer _valueSerializer;

    public RemovalNotificationValueHolder(final ByteBuffer value,
                                          final boolean compressed,
                                          final Serializer valueSerializer) {

        _value = value;
        _compressed = compressed;
        _valueSerializer = valueSerializer;
    }

    public Object getValue(){
        return _valueSerializer.deserialize(_value, _compressed);
    }
}
