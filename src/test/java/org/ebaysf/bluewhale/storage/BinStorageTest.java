package org.ebaysf.bluewhale.storage;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.ebaysf.bluewhale.configurable.CacheBuilder;
import org.ebaysf.bluewhale.configurable.EvictionStrategy;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.document.BinDocumentRaw;
import org.ebaysf.bluewhale.serialization.Serializers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executors;

/**
 * Created by huzhou on 2/28/14.
 */

public class BinStorageTest {

    private static final ListeningExecutorService _executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private static final EventBus _eventBus = new AsyncEventBus(_executor);

    @AfterClass
    public static void afterClass(){
        _executor.shutdownNow();
    }

    @Test(expected = IllegalArgumentException.class)
    @Ignore
    public void testBinStorageImpl() throws IOException {

        final File temp = Files.createTempDir();

        final BinStorage storage = new BinStorageImpl(new CacheBuilder.ConfigurationImpl(temp,
                    Serializers.BYTE_ARRAY_SERIALIZER, Serializers.BYTE_ARRAY_SERIALIZER, 1, 1, 1,
                    BinDocumentFactories.RAW, 1 << 20, 8, 2, 0.1f, 0.25f, null, false,
                    EvictionStrategy.SILENCE, _eventBus, _executor),
                Collections.<BinJournal>emptyList(),
                Mockito.mock(UsageTrack.class));

        Assert.assertNotNull(storage);

        Assert.assertEquals(temp, storage.local());
        Assert.assertEquals(1 << 20, storage.getJournalLength());
        Assert.assertEquals(8, storage.getMaxJournals());
        Assert.assertEquals(2, storage.getMaxMemoryMappedJournals());
        Assert.assertEquals(0, storage.getEvictedJournals());
        Assert.assertNotNull(storage.getUsageTrack());

        storage.read(0L);
    }

    @Test
    public void testBinStorageWriteAndRead() throws IOException {

        final File temp = Files.createTempDir();

        final BinStorage storage = new BinStorageImpl(new CacheBuilder.ConfigurationImpl(temp,
                    Serializers.BYTE_ARRAY_SERIALIZER, Serializers.BYTE_ARRAY_SERIALIZER, 1, 1, 1,
                    BinDocumentFactories.RAW, 1 << 20, 8, 2, 0.1f, 0.25f, null, false,
                    EvictionStrategy.SILENCE, _eventBus, _executor),
                Collections.<BinJournal>emptyList(),
                Mockito.mock(UsageTrack.class));

        final BinDocument small = new BinDocumentRaw()
                .setHashCode(1)
                .setKey(ByteBuffer.allocate(0))
                .setValue(ByteBuffer.allocate(0))
                .setLastModified(System.nanoTime())
                .setNext(-1L)
                .setState((byte)0x00);

        Assert.assertEquals(0L, storage.append(small));
        Assert.assertEquals(small, storage.read(0L));
    }

    @Test
    public void testOverflowAndIteration() throws IOException, InterruptedException {

        final File temp = Files.createTempDir();

        final BinStorageImpl storage = new BinStorageImpl(new CacheBuilder.ConfigurationImpl(temp,
                    Serializers.BYTE_ARRAY_SERIALIZER, Serializers.BYTE_ARRAY_SERIALIZER, 1, 1, 1,
                    BinDocumentFactories.RAW, 1 << 20, 8, 2, 0.1f, 0.25f, null, false,
                    EvictionStrategy.SILENCE, _eventBus, _executor),
                Collections.<BinJournal>emptyList(),
                Mockito.mock(UsageTrack.class));

        final BinJournal overflow = storage.nextWritable();

        Assert.assertNotNull(overflow);

        final BinDocument small = new BinDocumentRaw()
                .setHashCode(1)
                .setKey(ByteBuffer.allocate(0))
                .setValue(ByteBuffer.allocate(0))
                .setLastModified(System.nanoTime())
                .setNext(-1L)
                .setState((byte)0x00);

        int count = 0, pos = 0;
        for(int offset = overflow.append(small); offset >= 0; count += 1, pos += small.getLength(), offset = overflow.append(small)){
            Assert.assertEquals(small, overflow.read(offset));
        }

        Assert.assertEquals(count, overflow.getDocumentSize());
        Assert.assertEquals(pos, overflow.getMemoryMappedBuffer().limit());
        Assert.assertEquals(pos, overflow.getJournalLength());

        int meet = 0;
        for(Iterator<BinDocument> it = overflow.iterator(); it.hasNext(); meet += 1){
            Assert.assertEquals(small, it.next());
        }

        Assert.assertEquals(count, meet);

        final BinJournal immutable = storage.immutable(overflow);
        Assert.assertNotNull(immutable.usage());
        Assert.assertEquals(BinJournal.JournalState.BufferedReadOnly, immutable.currentState());
        Assert.assertEquals(overflow.getDocumentSize(), immutable.getDocumentSize());
        Assert.assertEquals(overflow.getJournalLength(), immutable.getJournalLength());
        Assert.assertEquals(overflow.usage().getLastModified(), immutable.usage().getLastModified());
        Assert.assertEquals(count, immutable.usage().getAlives().cardinality());

        meet = 0;
        for(Iterator<BinDocument> it = immutable.iterator(); it.hasNext(); meet += 1){
            Assert.assertEquals(small, it.next());
        }
        Assert.assertEquals(count, meet);

        final BinJournal downgrade = storage.downgrade(immutable);
        Assert.assertNotNull(downgrade);
        Assert.assertEquals(BinJournal.JournalState.FileChannelReadOnly, downgrade.currentState());
        Assert.assertEquals(immutable.getDocumentSize(), downgrade.getDocumentSize());
        Assert.assertEquals(immutable.getJournalLength(), downgrade.getJournalLength());
        Assert.assertSame(immutable.usage(), downgrade.usage());
        Assert.assertEquals(count, downgrade.usage().getAlives().cardinality());

        meet = 0;
        for(Iterator<BinDocument> it = downgrade.iterator(); it.hasNext(); meet += 1){
            Assert.assertEquals(small, it.next());
        }
        Assert.assertEquals(count, meet);

        Thread.sleep(30);
    }
}
