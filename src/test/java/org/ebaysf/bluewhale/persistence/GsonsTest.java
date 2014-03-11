package org.ebaysf.bluewhale.persistence;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Range;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.CacheImpl;
import org.ebaysf.bluewhale.configurable.CacheBuilder;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.segment.LeafSegment;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.segment.SegmentsManager;
import org.ebaysf.bluewhale.serialization.Serializers;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
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

        final Pair<Long, TimeUnit> origin = Pair.with(1L, TimeUnit.HOURS);
        final String str = gson.toJson(origin, Gsons.TTL_TYPE);

        Assert.assertNotNull(str);

        final Pair<Long, TimeUnit> parsed = gson.fromJson(str, Gsons.TTL_TYPE);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin.getValue1().toNanos(origin.getValue0().longValue()),
                parsed.getValue1().toNanos(parsed.getValue0().longValue()));
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

    @Test
    public void testGsonWithCache() throws Exception {

        final File temp = Files.createTempDir();

        final Cache<String, String> cache = CacheBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .local(temp)
                        .eventBus(new EventBus())
                        .executor(Executors.newCachedThreadPool())
                        .concurrencyLevel(2)
                        .maxSegmentDepth(2)
                        .binDocumentFactory(BinDocumentFactories.RAW)
                        .journalLength(1 << 20)
                        .maxJournals(8)
                        .maxMemoryMappedJournals(2)
                        .build();

        Assert.assertNotNull(cache);
        Assert.assertNull(cache.getIfPresent("key"));
        Assert.assertEquals("value", cache.get("key", new Callable<String>() {

            public @Override String call() throws Exception {
                return "value";
            }
        }));
        Assert.assertEquals("value", cache.getIfPresent("key"));

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final String str = gson.toJson(cache);
        Assert.assertNotNull(str);

        System.out.println(str);

        final PersistedCache parsed = gson.fromJson(str, PersistedCache.class);
        Assert.assertNotNull(parsed);
        Assert.assertNotNull(parsed.getConfiguration());
        Assert.assertNotNull(parsed.getPersistedSegments());
        Assert.assertNotNull(parsed.getPersistedJournals());

        final Cache<String, String> revived = new CacheImpl<String, String>(cache.getConfiguration(), new RemovalListener<String, String>() {
                public @Override void onRemoval(RemovalNotification<String, String> notification) {

                }
            },
            parsed.getPersistedSegments(),
            parsed.getPersistedJournals());

        Assert.assertNotNull(revived);
        Assert.assertEquals("value", cache.getIfPresent("key"));
    }
}
