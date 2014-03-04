package org.ebaysf.bluewhale.storage;

import com.google.common.collect.Range;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by huzhou on 2/27/14.
 */
public class ByteBufferBinJournal extends AbstractBinJournal {

    protected final ByteBuffer _mmap;

    public ByteBufferBinJournal(final File local,
                                final JournalState state,
                                final Range<Integer> journalRange,
                                final JournalsManager manager,
                                final BinDocumentFactory factory,
                                final int length,
                                final ByteBuffer buffer) {

        super(local, state, journalRange, manager, factory, length);

        _mmap = buffer;

        _manager.rememberBufferUsedByjournal(_mmap, this);
    }

    public @Override int append(final BinDocument document) throws IOException {

        throw new UnsupportedOperationException("None writable!");
    }

    public @Override BinDocument read(final int offset) throws IOException {

        return _factory.getReader(_mmap, offset).read();
    }

    public @Override ByteBuffer getMemoryMappedBuffer() {

        return _mmap;
    }

    public @Override Iterator<BinDocument> iterator() {
        try {
            return new BinDocumentIterator(_mmap, 0);
        }
        catch (IOException e) {
            return Collections.<BinDocument>emptyList().iterator();
        }
    }

    protected class BinDocumentIterator implements Iterator<BinDocument> {

        private final ByteBuffer _buffer;
        private int _offset;
        private BinDocument _doc;

        public BinDocumentIterator(final ByteBuffer buffer, final int offset) throws IOException {
            _buffer = buffer;
            _offset = offset;
            _doc = _factory.getReader(buffer, _offset).verify();
        }

        public @Override boolean hasNext() {
            return _doc != null;
        }

        public @Override BinDocument next() {
            final BinDocument doc = _doc;
            final int length = doc.getLength();
            _offset += length;
            if(_offset < _buffer.limit()){
                try {
                    _doc = _factory.getReader(_buffer, _offset).verify();
                }
                catch (IOException e) {
                    _doc = null;
                }
            }
            return doc;
        }

        public @Override void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
