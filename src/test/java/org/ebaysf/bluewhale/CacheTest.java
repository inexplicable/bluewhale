package org.ebaysf.bluewhale;

import com.google.common.base.Strings;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import org.ebaysf.bluewhale.configurable.CacheBuilder;
import org.ebaysf.bluewhale.configurable.EvictionStrategy;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.serialization.Serializers;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by huzhou on 3/3/14.
 */
public class CacheTest {

    private static final ExecutorService _executor = Executors.newCachedThreadPool();

    @AfterClass
    public static void afterClass(){
        _executor.shutdownNow();
    }

    @Test
    public void testCacheImpl() throws IOException, ExecutionException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new EventBus();

        final Cache<String, String> cache = CacheBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .local(temp)
                        .eventBus(eventBus)
                        .executor(_executor)
                        .concurrencyLevel(2)
                        .maxSegmentDepth(2)
                        .binDocumentFactory(BinDocumentFactories.RAW)
                        .journalLength(1 << 20)
                        .maxJournals(8)
                        .maxMemoryMappedJournals(2)
                        .persists(false)
                        .build();

        Assert.assertNotNull(cache);
        Assert.assertNull(cache.getIfPresent("key"));
        Assert.assertEquals("value", cache.get("key", new Callable<String>() {

            public @Override String call() throws Exception {
                return "value";
            }
        }));
        Assert.assertEquals("value", cache.getIfPresent("key"));
    }

    @Test
    public void testCacheImplRemoval() throws IOException, ExecutionException, InterruptedException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new EventBus();

        final List<RemovalCause> actualCauses = Lists.newLinkedList();
        final List<String> actualRemovedValues = Lists.newLinkedList();
        final Cache<String, String> cache = CacheBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                .local(temp)
                .eventBus(eventBus)
                .executor(_executor)
                .concurrencyLevel(2)
                .maxSegmentDepth(2)
                .binDocumentFactory(BinDocumentFactories.RAW)
                .journalLength(1 << 20)
                .maxJournals(8)
                .maxMemoryMappedJournals(2)
                .persists(false)
                .removalListener(new RemovalListener<String, String>() {
                    public @Override void onRemoval(final RemovalNotification<String, String> notification) {
                        actualCauses.add(notification.getCause());
                        actualRemovedValues.add(notification.getValue());
                    }
                })
                .build();

        Assert.assertNotNull(cache);
        Assert.assertNull(cache.getIfPresent("key"));
        Assert.assertEquals("value", cache.get("key", new Callable<String>() {

            public @Override String call() throws Exception {
                return "value";
            }
        }));
        Assert.assertEquals("value", cache.getIfPresent("key"));

        cache.invalidate("key");

        cache.put("key", "value");
        cache.put("key", "value-modified");

        cache.invalidate("key");

        Assert.assertEquals(Arrays.asList(RemovalCause.EXPLICIT, RemovalCause.REPLACED, RemovalCause.EXPLICIT), actualCauses);
        Assert.assertEquals(Arrays.asList("value", "value", "value-modified"), actualRemovedValues);

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void testCacheImplPathShorten() throws IOException, ExecutionException, InterruptedException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new EventBus();

        final AtomicBoolean isValueCompressed = new AtomicBoolean(false);

        final Serializer<String> sameHashStringSerializer = new Serializers.AbstractBinComparableSerializer<String>() {

            public @Override int hashCode(final String object){
                return 1;//always 1;
            }

            public @Override ByteBuffer serialize(final String object) {
                return Serializers.STRING_SERIALIZER.serialize(object);
            }

            public @Override String deserialize(final ByteBuffer binaries,
                                                final boolean compressed) {

                isValueCompressed.set(compressed);
                return Serializers.STRING_SERIALIZER.deserialize(binaries, compressed);
            }
        };

        final Cache<String, String> cache = CacheBuilder.builder(sameHashStringSerializer, sameHashStringSerializer)
                .local(temp)
                .eventBus(eventBus)
                .executor(_executor)
                .concurrencyLevel(2)
                .maxSegmentDepth(2)
                .binDocumentFactory(BinDocumentFactories.RAW)
                .journalLength(1 << 20)
                .maxJournals(8)
                .maxMemoryMappedJournals(2)
                .persists(false)
                .removalListener(new RemovalListener<String, String>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, String> notification) {

                        System.out.println(notification);
                    }
                })
                .build();

        Assert.assertNotNull(cache);
        Assert.assertEquals("value-buried", cache.get("key-buried", new Callable<String>() {
            public @Override String call() throws Exception {
                return "value-buried";
            }
        }));

        for(int i = 0; i < cache.getConfiguration().getMaxPathDepth(); i +=1){
            cache.put("key", "value");
        }

        Assert.assertEquals("value-buried", cache.getIfPresent("key-buried"));
        Assert.assertFalse(isValueCompressed.get());
        Assert.assertEquals("value-buried", cache.getIfPresent("key-buried"));
        Assert.assertTrue(isValueCompressed.get());

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void testCacheImplUsageTrack() throws IOException, ExecutionException, InterruptedException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new EventBus();
        final Serializer<String> strSerializer = Serializers.STRING_SERIALIZER;

        final AtomicReference<Segment> segmentRef = new AtomicReference<Segment>(null);
        final AtomicReference<BinDocument> documentRef = new AtomicReference<BinDocument>(null);

        final Cache<String, String> cache = CacheBuilder.builder(strSerializer, strSerializer)
                .local(temp)
                .eventBus(eventBus)
                .executor(_executor)
                .concurrencyLevel(2)
                .maxSegmentDepth(2)
                .binDocumentFactory(BinDocumentFactories.RAW)
                .journalLength(1 << 20)
                .maxJournals(8)
                .maxMemoryMappedJournals(2)
                .persists(false)
                .evictionStrategy(new EvictionStrategy() {

                    public @Override void afterGet(Segment segment, BinStorage storage, long token, BinDocument doc) {

                    }

                    public @Override void afterPut(Segment segment, BinStorage storage, long token, BinDocument doc) {
                        segmentRef.set(segment);
                        documentRef.set(doc);
                    }
                })
                .build();

        Assert.assertNotNull(cache);
        Assert.assertNull(cache.getIfPresent("key"));
        Assert.assertEquals("value", cache.get("key", new Callable<String>() {
            public @Override String call() throws Exception {
                return "value";
            }
        }));
        Assert.assertEquals("value", cache.getIfPresent("key"));

        final Segment originalSegment = segmentRef.get();
        final BinDocument originalDocument = documentRef.get();
        Assert.assertNotNull(originalSegment);
        Assert.assertNotNull(originalDocument);
        Assert.assertTrue(originalSegment.using(originalDocument));

        cache.put("key", "value-modified");
        final Segment segmentAfterOverwrite = segmentRef.get();
        final BinDocument documentAfterOverwrite = documentRef.get();

        Assert.assertSame(originalSegment, segmentAfterOverwrite);
        Assert.assertNotSame(originalDocument, documentAfterOverwrite);
        Assert.assertTrue(segmentAfterOverwrite.using(documentAfterOverwrite));
        Assert.assertFalse(segmentAfterOverwrite.using(originalDocument));

        cache.invalidate("key");

        final Segment segmentAfterInvalidate = segmentRef.get();
        final BinDocument documentAfterInvalidate = documentRef.get();

        Assert.assertSame(originalSegment, segmentAfterInvalidate);
        Assert.assertNotSame(originalDocument, documentAfterInvalidate);
        Assert.assertTrue(segmentAfterInvalidate.using(documentAfterInvalidate));
        Assert.assertFalse(segmentAfterInvalidate.using(originalDocument));
        Assert.assertFalse(segmentAfterInvalidate.using(documentAfterOverwrite));


        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void testCachePerf() throws IOException, ExecutionException, InterruptedException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new EventBus();

        final Cache<String, String> cache = CacheBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .local(temp)
                        .eventBus(eventBus)
                        .executor(_executor)
                        .concurrencyLevel(3)
                        .maxSegmentDepth(4)
                        .binDocumentFactory(BinDocumentFactories.RAW)
                        .journalLength(1 << 20)
                        .maxJournals(8)
                        .maxMemoryMappedJournals(2)
                        .persists(false)
                        .build();

        final String[] candidates = new String[100000];
        for(int i = 0; i < candidates.length; i += 1){
            candidates[i] = Strings.padStart(String.valueOf(i), String.valueOf(candidates.length).length() + 1, '*');//10bytes apprx
        }

        Assert.assertNotNull(cache);

        long begin = System.nanoTime();
        for(final String c : candidates){
            Assert.assertNull(cache.getIfPresent(c));
        }

        System.out.printf("[sequential] 1k sized cache, 100bytes key, 100bytes value, getIfPresent took: %dns\n", (System.nanoTime() - begin) / candidates.length);

        begin = System.nanoTime();
        for(final String c : candidates){
            Assert.assertEquals(c, cache.get(c, new Callable<String>() {
                public @Override String call() throws Exception {
                    return c;
                }
            }));
        }

        System.out.printf("[sequential] 1k sized cache, 100bytes key, 100bytes value, get took: %dns\n", (System.nanoTime() - begin) / candidates.length);

        begin = System.nanoTime();
        for(final String c : candidates){
            Assert.assertEquals(c, cache.getIfPresent(c));
        }

        System.out.printf("[sequential] 1k sized cache, 100bytes key, 100bytes value, getIfPresent took: %dns\n", (System.nanoTime() - begin) / candidates.length);

        Thread.sleep(1000);
    }

    @Test
    public void testConcurrentCachePerf() throws IOException, ExecutionException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new EventBus();
        final AtomicLong durations = new AtomicLong(0L);
        final ExecutorService concurrency = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2 + 1);

        final Cache<String, String> cache = CacheBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .local(temp)
                        .eventBus(eventBus)
                        .executor(_executor)
                        .concurrencyLevel(2)
                        .maxSegmentDepth(2)
                        .binDocumentFactory(BinDocumentFactories.RAW)
                        .journalLength(1 << 20)
                        .maxJournals(8)
                        .maxMemoryMappedJournals(2)
                        .persists(false)
                        .build();

        final String[] candidates = new String[10000];
        for(int i = 0; i < candidates.length; i += 1){
            candidates[i] = Strings.padStart(String.valueOf(i), 100, '*');//100bytes apprx
        }

        Assert.assertNotNull(cache);

        final List<Future<?>> getIfPresentsFutures = Lists.newArrayListWithExpectedSize(candidates.length);
        for(final String c : candidates){
            getIfPresentsFutures.add(concurrency.submit(new Runnable() {
                public @Override void run() {
                    final long before = System.nanoTime();
                    Assert.assertNull(cache.getIfPresent(c));
                    durations.getAndAdd(System.nanoTime() - before);
                }
            }));
        }
        while(!getIfPresentsFutures.isEmpty()){
            for(final Iterator<Future<?>> it = getIfPresentsFutures.iterator(); it.hasNext();){
                if(it.next().isDone()){
                    it.remove();
                }
            }
            Thread.yield();
        }

        System.out.printf("[concurrent] 10k sized cache, 100bytes key, 100bytes value, getIfPresent took: %dns\n", durations.getAndSet(0L) / candidates.length);

        final List<Future<?>> getsFutures = Lists.newArrayListWithExpectedSize(candidates.length);
        for(final String c : candidates){
            getsFutures.add(concurrency.submit(new Runnable() {
                public @Override void run() {
                    final long before = System.nanoTime();
                    try {
                        Assert.assertEquals(c, cache.get(c, new Callable<String>() {
                            public @Override String call() throws Exception {
                                return c;
                            }
                        }));
                    }
                    catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    durations.getAndAdd(System.nanoTime() - before);
                }
            }));
        }
        while(!getsFutures.isEmpty()){
            for(final Iterator<Future<?>> it = getsFutures.iterator(); it.hasNext();){
                if(it.next().isDone()){
                    it.remove();
                }
            }
            Thread.yield();
        }

        System.out.printf("[concurrent] 10k sized cache, 100bytes key, 100bytes value, get took: %dns\n", durations.getAndSet(0L) / candidates.length);

        for(final String c : candidates){
            Assert.assertEquals(c, cache.getIfPresent(c));
        }

        getIfPresentsFutures.clear();
        for(final String c : candidates){
            getIfPresentsFutures.add(concurrency.submit(new Runnable() {
                public @Override void run() {
                    final long before = System.nanoTime();
                    Assert.assertEquals(c, cache.getIfPresent(c));
                    durations.getAndAdd(System.nanoTime() - before);
                }
            }));
        }
        while(!getIfPresentsFutures.isEmpty()){
            for(final Iterator<Future<?>> it = getIfPresentsFutures.iterator(); it.hasNext();){
                if(it.next().isDone()){
                    it.remove();
                }
            }
            Thread.yield();
        }

        System.out.printf("[concurrent] 10k sized cache, 100bytes key, 100bytes value, getIfPresent took: %dns\n", durations.getAndSet(0L) / candidates.length);

        concurrency.shutdown();

        Assert.assertNotNull(cache.stats());
    }
}
