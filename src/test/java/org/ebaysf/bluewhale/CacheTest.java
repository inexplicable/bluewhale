package org.ebaysf.bluewhale;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.serialization.Serializers;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Created by huzhou on 3/3/14.
 */
public class CacheTest {

    private static final ListeningExecutorService _executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private static final EventBus _eventBus = new AsyncEventBus(_executor);

    @AfterClass
    public static void afterClass(){
        _executor.shutdownNow();
    }

    @Test
    public void testCacheImpl() throws IOException, ExecutionException {

        final File temp = Files.createTempDir();

        final Cache<String, String> cache = new CacheImpl<String, String>(temp,
                2,
                Serializers.STRING_SERIALIZER,
                Serializers.STRING_SERIALIZER,
                _eventBus,
                _executor,
                new RemovalListener<String, String>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, String> notification) {

                    }
                },
                BinDocumentFactories.RAW,
                1 << 20,//1MB JOURNAL LENGTH
                8,  //8MB TOTAL JOURNAL BYTES
                2,
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
}
