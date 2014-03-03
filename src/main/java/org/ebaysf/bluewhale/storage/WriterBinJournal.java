package org.ebaysf.bluewhale.storage;

import com.google.common.collect.Range;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huzhou on 2/27/14.
 */
public class WriterBinJournal extends ByteBufferBinJournal {

    private final AtomicInteger _offset;

    public WriterBinJournal(final File local,
                            final Range<Integer> journalRange,
                            final JournalsManager manager,
                            final BinDocumentFactory factory,
                            final int length,
                            final ByteBuffer buffer) {

        super(local, JournalState.BufferedWritable, journalRange, manager, factory, length, buffer);

        _offset = new AtomicInteger(0);
    }

    public @Override int append(final BinDocument document) throws IOException {

        final BinDocumentFactory.BinDocumentWriter writer = _factory.getWriter(document);

        final int length = writer.length();
        final int offset = _offset.getAndAdd(length);

        //-1 when the mmap is filled up
        if(offset + length >= _mmap.limit()){
            //disallow further write after offset immediately
            _mmap.limit(offset);
            return -1;
        }
        //do the actual write when there's enough buffer
        else{
            writer.write(_mmap, offset);
            _size += 1;
            return offset;
        }
    }

}
