package org.ebaysf.bluewhale.segment;

import com.google.common.collect.Range;
import com.google.common.math.DoubleMath;
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
    int SIZE = (1 << 15) * (Long.SIZE / Byte.SIZE);
    int MASK_OF_OFFSET = -1 >>> 17;// least 15 bits
    int MAX_TOKENS_IN_ONE_SEGMENT = DoubleMath.roundToInt(Segment.SIZE / Long.SIZE * SPLIT_THRESHOLD, RoundingMode.FLOOR);// number of slots
    int MAX_SEGMENTS = 1 << 16;

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
