package org.ebaysf.bluewhale.util;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: huzhou
 * Date: 12/12/12
 * Time: 9:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class Files {

    private static final Random _random = new Random(System.currentTimeMillis());
    
    public static File newCacheFile(final File dir) throws IOException {
    	final File cacheFile = new File(dir,
    			new StringBuilder().append(System.currentTimeMillis()).append('-').append(_random.nextInt()).append(".cold").toString());

        cacheFile.createNewFile();
        return cacheFile;
    }

    public static File newSegmentFile(final File dir, final boolean deleteOnExit) throws IOException {
        final File segmentFile = new File(dir,
                new StringBuilder().append(System.currentTimeMillis()).append('-').append(_random.nextInt()).append(".segment").toString());

        Preconditions.checkState(!segmentFile.exists());
        segmentFile.createNewFile();

        if(deleteOnExit){
            segmentFile.deleteOnExit();
        }

        return segmentFile;
    }

    public static File newJournalFile(final File dir, final boolean deleteOnExit) throws IOException {
        final File journalFile = new File(dir,
                new StringBuilder().append(System.currentTimeMillis()).append('-').append(_random.nextInt()).append(".journal").toString());

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
            e.printStackTrace();
        }
    }
}
