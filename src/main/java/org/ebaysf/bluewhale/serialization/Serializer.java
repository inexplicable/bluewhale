package org.ebaysf.bluewhale.serialization;

import java.nio.ByteBuffer;

/**
 * Created by huzhou on 2/26/14.
 */
public interface Serializer<E> {

    ByteBuffer serialize(final E object);

    E deserialize(final ByteBuffer binaries, final boolean compressed);

    int hashCode(final E object);

    boolean equals(final ByteBuffer target, final ByteBuffer other);

    boolean equals(final E target, final E other);

    boolean equals(final ByteBuffer target, final E other);

    boolean equals(final E target, final ByteBuffer other);
}
