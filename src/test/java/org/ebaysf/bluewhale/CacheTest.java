package org.ebaysf.bluewhale;

import com.google.common.base.Strings;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import org.ebaysf.bluewhale.configurable.ConfigurationBuilder;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.serialization.Serializers;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

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

        final Cache<String, String> cache = new CacheImpl<String, String>(
                ConfigurationBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .setLocal(temp)
                        .setEventBus(eventBus)
                        .setExecutor(_executor)
                        .setConcurrencyLevel(2)
                        .setMaxSegmentDepth(2)
                        .setBinDocumentFactory(BinDocumentFactories.RAW)
                        .setJournalLength(1 << 20)
                        .setMaxJournals(8)
                        .setMaxMemoryMappedJournals(2)
                        .setCleanUpOnExit(true)
                        .build(),
                new RemovalListener<String, String>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, String> notification) {

                    }
                },
                Collections.<BinJournal>emptyList());

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

        final Cache<String, String> cache = new CacheImpl<String, String>(
                ConfigurationBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .setLocal(temp)
                        .setEventBus(eventBus)
                        .setExecutor(_executor)
                        .setConcurrencyLevel(2)
                        .setMaxSegmentDepth(2)
                        .setBinDocumentFactory(BinDocumentFactories.RAW)
                        .setJournalLength(1 << 20)
                        .setMaxJournals(8)
                        .setMaxMemoryMappedJournals(2)
                        .setCleanUpOnExit(true)
                        .build(),
                new RemovalListener<String, String>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, String> notification) {

                        System.out.println(notification);
                    }
                },
                Collections.<BinJournal>emptyList());

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

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void testCachePerf() throws IOException, ExecutionException, InterruptedException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new AsyncEventBus(_executor);

        final Cache<String, String> cache = new CacheImpl<String, String>(
                ConfigurationBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .setLocal(temp)
                        .setEventBus(eventBus)
                        .setExecutor(_executor)
                        .setConcurrencyLevel(2)
                        .setMaxSegmentDepth(2)
                        .setBinDocumentFactory(BinDocumentFactories.RAW)
                        .setJournalLength(1 << 20)
                        .setMaxJournals(8)
                        .setMaxMemoryMappedJournals(2)
                        .setCleanUpOnExit(true)
                        .build(),
                new RemovalListener<String, String>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, String> notification) {

                    }
                },
                Collections.<BinJournal>emptyList());

        final String[] candidates = new String[1000];
        for(int i = 0; i < candidates.length; i += 1){
            candidates[i] = Strings.padStart(String.valueOf(i), 10, '*');//10bytes apprx
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

    }

    @Test
    public void testConcurrentCachePerf() throws IOException, ExecutionException {

        final File temp = Files.createTempDir();
        final EventBus eventBus = new AsyncEventBus(_executor);
        final AtomicLong durations = new AtomicLong(0L);
        final ExecutorService concurrency = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2 + 1);

        final Cache<String, String> cache = new CacheImpl<String, String>(
                ConfigurationBuilder.builder(Serializers.STRING_SERIALIZER, Serializers.STRING_SERIALIZER)
                        .setLocal(temp)
                        .setEventBus(eventBus)
                        .setExecutor(_executor)
                        .setConcurrencyLevel(2)
                        .setMaxSegmentDepth(2)
                        .setBinDocumentFactory(BinDocumentFactories.RAW)
                        .setJournalLength(1 << 20)
                        .setMaxJournals(8)
                        .setMaxMemoryMappedJournals(2)
                        .setCleanUpOnExit(true)
                        .build(),
                new RemovalListener<String, String>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, String> notification) {

                    }
                },
                Collections.<BinJournal>emptyList());

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
    }
}
