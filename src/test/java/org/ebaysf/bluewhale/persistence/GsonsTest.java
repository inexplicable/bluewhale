package org.ebaysf.bluewhale.persistence;

import com.google.common.collect.Range;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.brettw.SparseBitSet;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.segment.LeafSegment;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.segment.SegmentsManager;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by huzhou on 3/7/14.
 */
public class GsonsTest {

    @Test
    public void testGsonWithFiles(){

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final File origin = Files.createTempDir();
        final String str = gson.toJson(origin);

        Assert.assertNotNull(str);

        final File parsed = gson.fromJson(str, File.class);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin.getAbsolutePath(), parsed.getAbsolutePath());

        origin.delete();
    }

    @Test
    public void testGsonWithPairs(){

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final Pair<Long, TimeUnit> origin = new Pair<Long, TimeUnit>(1L, TimeUnit.HOURS);
        final String str = gson.toJson(origin);

        Assert.assertNotNull(str);

        final Pair<Long, TimeUnit> parsed = gson.fromJson(str, Pair.class);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin.getValue1().toNanos(origin.getValue0().longValue()),
                parsed.getValue1().toNanos(parsed.getValue0().longValue()));
    }

    @Test
    public void testGsonWithSparseBitSet(){

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final SparseBitSet origin = new SparseBitSet();
        for(int i = 0; i < 1000000; i += 3){
            origin.set(i);
        }
        final String str = gson.toJson(origin);

        Assert.assertNotNull(str);

        final SparseBitSet parsed = gson.fromJson(str, SparseBitSet.class);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin, parsed);

    }

    @Test
    public void testGsonWithLeafSegment(){

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final File source = Files.createTempDir();
        final Configuration psuedoConfiguration = Mockito.mock(Configuration.class);
        Mockito.when(psuedoConfiguration.getEventBus()).thenReturn(new EventBus());

        final LeafSegment origin = new LeafSegment(source,
                Range.singleton(1),
                psuedoConfiguration,
                Mockito.mock(SegmentsManager.class),
                Mockito.mock(BinStorage.class),
                ByteBuffer.allocate(0),
                1);

        Assert.assertNotNull(origin);

        final String str = gson.toJson(origin);

        final Segment parsed = gson.fromJson(str, Segment.class);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin.local(), parsed.local());
        Assert.assertEquals(origin.range(), parsed.range());
        Assert.assertEquals(origin.size(), parsed.size());
    }
}
