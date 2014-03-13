package org.ebaysf.bluewhale.segment;

import com.google.common.collect.Range;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Longs;
import org.ebaysf.bluewhale.command.Get;
import org.ebaysf.bluewhale.command.Put;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.ebaysf.bluewhale.storage.UsageTrack;

import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by huzhou on 2/26/14.
 */
public interface Segment extends UsageTrack {

    double SPLIT_THRESHOLD = 0.75d;
    int BYTES = (1 << (Short.SIZE - 1)) * Longs.BYTES; //bytes
    int MASK_OF_OFFSET = -1 >>> (Short.SIZE + 1);//least 15 bits mask
    int MAX_TOKENS_IN_ONE_SEGMENT = DoubleMath.roundToInt(Segment.BYTES / Longs.BYTES * SPLIT_THRESHOLD, RoundingMode.FLOOR);// number of slots
    int MAX_SEGMENTS = 1 << Short.SIZE;

    Configuration configuration();

    File local();

    Range<Integer> range();

    List<Segment> getChildren();

    boolean isLeaf();

    int size();

    <K> Serializer<K> getKeySerializer();

    <V> Serializer<V> getValSerializer();

    BinStorage getStorage();

    Segment route(final int segmentCode);

    void put(final Put put) throws IOException;

    <V> V get(final Get get) throws ExecutionException, IOException;
}
