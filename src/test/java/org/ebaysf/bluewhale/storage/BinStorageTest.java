package org.ebaysf.bluewhale.storage;

import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.document.BinDocumentRaw;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Executors;

/**
 * Created by huzhou on 2/28/14.
 */

public class BinStorageTest {

    private static final ListeningExecutorService _executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

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
                Collections.<BinJournal>emptyList(),
                _executor);

        Assert.assertNotNull(storage);

        Assert.assertEquals(temp, storage.local());
        Assert.assertEquals(1 << 20, storage.getJournalLength());
        Assert.assertEquals(8, storage.getMaxJournals());
        Assert.assertEquals(0, storage.getEvictedJournals());

        storage.read(0L);
    }

    @Test
    public void testBinStorageWriteAndRead() throws IOException {

        final File temp = Files.createTempDir();

        final BinStorage storage = new BinStorageImpl(temp,
                BinDocumentFactories.RAW,
                1 << 20,//1MB JOURNAL LENGTH
                8,  //8MB TOTAL JOURNAL BYTES
                Collections.<BinJournal>emptyList(),
                _executor);

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
}
