package org.ebaysf.bluewhale.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: huzhou
 * Date: 12/12/12
 * Time: 9:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class Files {

    private static final Logger LOG = LoggerFactory.getLogger(Files.class);

    private static final String TERM = String.valueOf(System.currentTimeMillis());
    private static final AtomicInteger SEED = new AtomicInteger(0);
    private static final int PADDING = String.valueOf(Integer.MAX_VALUE).length();

    private static String randomFileName(final String postfix){
        return new StringBuilder().append(TERM).append('-').append(Strings.padStart(String.valueOf(SEED.getAndIncrement()), PADDING, '0')).append(postfix).toString();
    }

    public static File newCacheFile(final File dir) throws IOException {

    	final File cacheFile = new File(dir, randomFileName(".cold"));
        cacheFile.createNewFile();
        return cacheFile;
    }

    public static File newSegmentFile(final File dir, final boolean deleteOnExit) throws IOException {

        final File segmentFile = new File(dir, randomFileName(".segment"));

        Preconditions.checkState(!segmentFile.exists());
        segmentFile.createNewFile();

        if(deleteOnExit){
            segmentFile.deleteOnExit();
        }

        return segmentFile;
    }

    public static File newJournalFile(final File dir, final boolean deleteOnExit) throws IOException {

        final File journalFile = new File(dir, randomFileName(".journal"));

        Preconditions.checkState(!journalFile.exists());
        journalFile.createNewFile();

        if(deleteOnExit){
            journalFile.deleteOnExit();
        }

        return journalFile;
    }

    public static void freeUpBuffer(final ByteBuffer bufferNoLongerUsed) {

        Preconditions.checkArgument(bufferNoLongerUsed.isDirect(), "bufferNoLongerUsed isn't direct!");

        try {

            Method cleanerMethod = bufferNoLongerUsed.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(bufferNoLongerUsed);
            Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.setAccessible(true);
            cleanMethod.invoke(cleaner);

        }
        catch (Exception e) {

            LOG.error("byte buffer immediate cleanup failed", e);
        }
    }
}
