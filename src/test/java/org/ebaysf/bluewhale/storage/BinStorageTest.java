package org.ebaysf.bluewhale.storage;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.document.BinDocumentRaw;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
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
    public void testBinStorageImpl() throws IOException {

        final File temp = Files.createTempDir();

        final BinStorage storage = new BinStorageImpl(temp,
                BinDocumentFactories.RAW,
                1 << 20,//1MB JOURNAL LENGTH
                8,  //8MB TOTAL JOURNAL BYTES
                2,
                Collections.<BinJournal>emptyList(),
                _eventBus,
                _executor,
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

        final BinStorage storage = new BinStorageImpl(temp,
                BinDocumentFactories.RAW,
                1 << 20,//1MB JOURNAL LENGTH
                8,  //8MB TOTAL JOURNAL BYTES
                2,
                Collections.<BinJournal>emptyList(),
                _eventBus,
                _executor,
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

        final BinStorageImpl storage = new BinStorageImpl(temp,
                BinDocumentFactories.RAW,
                1 << 10,//1KB JOURNAL LENGTH
                8,  //8MB TOTAL JOURNAL BYTES
                2,
                Collections.<BinJournal>emptyList(),
                _eventBus,
                _executor,
                Mockito.mock(UsageTrack.class));

        final BinJournal overflow = storage.nextWritable(temp);

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

        Assert.assertEquals(pos, overflow.getMemoryMappedBuffer().limit());

        int meet = 0;
        for(Iterator<BinDocument> it = overflow.iterator(); it.hasNext(); meet += 1){
            Assert.assertEquals(small, it.next());
        }

        Assert.assertEquals(count, meet);

        final BinJournal downgrade = new FileChannelBinJournal(overflow.local(),
                overflow.range(),
                storage._manager,
                overflow.usage(),
                storage._factory,
                overflow.getJournalLength(),
                overflow.getDocumentSize(),
                -1);

        Assert.assertNotNull(downgrade);

        meet = 0;
        for(Iterator<BinDocument> it = downgrade.iterator(); it.hasNext(); meet += 1){
            Assert.assertEquals(small, it.next());
        }

        Assert.assertEquals(count, meet);
    }
}
