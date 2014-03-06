package org.ebaysf.bluewhale.storage;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.eventbus.Subscribe;
import com.google.common.math.IntMath;
import com.google.common.primitives.Longs;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.event.DocumentLengthAnticipatedEvent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 * Created by huzhou on 2/27/14.
 */
public class FileChannelBinJournal extends AbstractBinJournal {

    private static final Logger LOG = Logger.getLogger(FileChannelBinJournal.class.getName());

    protected final RandomAccessFile _raf;
    protected final FileChannel _fch;
    protected int _documentLength90 = 256;

    public FileChannelBinJournal(final File local,
                                 final Range<Integer> journalRange,
                                 final JournalsManager manager,
                                 final JournalUsage usage,
                                 final BinDocumentFactory factory,
                                 final int length,
                                 final int size,
                                 final int documentLength90) throws FileNotFoundException {

        super(local, JournalState.FileChannelReadOnly, journalRange, manager, usage, factory, length);

        _raf = new RandomAccessFile(local(), "r");
        _fch = _raf.getChannel();
        _size = size;

        if(documentLength90 < 0){
            _manager.getEventBus().register(this);
            _manager.getExecutor().submit(new Runnable() {
                @Override
                public void run() {

                    final BinJournal self = FileChannelBinJournal.this;

                    LOG.info("[anticipating] ".concat(self.toString()));
                    //use average length 1st, we'll then use 90% or better to find a better idea.
                    final int mostLengthy = IntMath.divide(self.getDocumentSize() * 9, 10, RoundingMode.FLOOR);
                    Preconditions.checkState(mostLengthy > 0);

                    final PriorityQueue<Long> minHeap = new PriorityQueue<Long>(mostLengthy);
                    final Iterator<BinDocument> it = self.iterator();

                    while(it.hasNext() && minHeap.size() <= mostLengthy){
                        BinDocument doc = it.next();
                        minHeap.offer(Long.valueOf(doc.getLength()));
                    }
                    while(it.hasNext()){
                        BinDocument doc = it.next();
                        final Long length = Long.valueOf(doc.getLength());
                        if(minHeap.peek().longValue() < length.longValue()){
                            minHeap.poll();
                            minHeap.offer(length);
                        }
                    }
                    //return the least/head of the minHeap, which is larger than 90% of the documents in this journal
                    final Integer greaterThan90Percents =  minHeap.poll().intValue();
                    LOG.info(new StringBuilder()
                            .append("[anticipated] ").append(self).append(" will use:")
                            .append(greaterThan90Percents).append(" Bytes for future reads").toString());

                    _manager.getEventBus().post(new DocumentLengthAnticipatedEvent(FileChannelBinJournal.this, greaterThan90Percents.intValue()));
                }
            });
        }
        else{
            _documentLength90 = documentLength90;
        }
    }

    public @Override int append(final BinDocument document) throws IOException {

        throw new UnsupportedOperationException("None writable!");
    }

    public @Override BinDocument read(int offset) throws IOException {

        return _factory.getReader(_fch, offset, _documentLength90).read();
    }

    public @Override ByteBuffer getMemoryMappedBuffer() {

        throw new UnsupportedOperationException("FileChannel based!");
    }

    public @Override Iterator<BinDocument> iterator() {

        try{
            return new BinDocumentBlockIterator(0);
        }
        catch(IOException e){
            LOG.warning(Throwables.getStackTraceAsString(e));
            return Iterators.emptyIterator();
        }
    }

    @Subscribe
    public void onDocumentLengthAnticipated(final DocumentLengthAnticipatedEvent event){
        if(event.getSource() == this){
            _manager.getEventBus().unregister(this);
            _documentLength90 = event.getDocumentLengthAnticipated();
        }
    }

    /**
     * will test against BinDocumentIterator see if it really iterates much faster, well, it should, given the much larger block size.
     */
    protected class BinDocumentBlockIterator implements Iterator<BinDocument> {

        private static final int MAX_BLOCK_SIZE = 1 << 16;//64k size block

        private final long _fileLength;
        private final ByteBuffer _block;

        private int _offsetAtFile;
        private BinDocument _next;

        public BinDocumentBlockIterator(final int offset) throws IOException{
            _fileLength = getJournalLength();
            //block size must be [anticipatedLength, MAX_BLOCK_SIZE]
            _block = ByteBuffer.allocate(Math.max(_documentLength90, Math.min(MAX_BLOCK_SIZE, _documentLength90 << 4)));

            _offsetAtFile = offset;
            _block.limit(Math.min(_block.capacity(), getJournalLength()));
            _offsetAtFile += _fch.read(_block, offset);
            _block.rewind();
            _next = readNext();
        }

        private BinDocument readNext(){
            try{
                final int mark = _block.position();
                final BinDocumentFactory.BinDocumentReader lookAhead = _block.remaining() >= Longs.BYTES
                        ? _factory.getReader(_block, mark)
                        : null;
                if(lookAhead == null){//cannot read 8 bytes, but fch still available
                    if(_offsetAtFile < _fileLength){
                        refreshBlock(mark);
                        return readNext();
                    }
                }
                else if(lookAhead.getLength() <= 0){
                    //odd case, possibly due to uninitialized file region, could calc negative lenght, which caused
                    //infinite iterations.
                    return null;
                }
                else if(lookAhead.getLength() > _block.limit()){
                    //special case, when the document is so long that a single block couldn't hold it
                    final ByteBuffer enough = ByteBuffer.allocate(lookAhead.getLength());
                    _offsetAtFile -= _block.limit() - mark;
                    _offsetAtFile += _fch.read(enough, _offsetAtFile);
                    final BinDocument large = _factory.getReader(enough, 0).verify();
                    if(large != null){
                        _block.rewind();
                        _offsetAtFile += _fch.read(_block, _offsetAtFile);
                        _block.rewind();
                    }
                    return large;
                }
                else if(mark + lookAhead.getLength() > _block.limit()){//read 8 bytes, found document length exceed block
                    refreshBlock(mark);
                    return readNext();
                }
                else if(mark + lookAhead.getLength() <= _block.limit()){//document length within block limit
                    _block.position(mark + lookAhead.getLength());
                    return lookAhead.read();
                }
            }
            catch (IOException e) {
                LOG.warning(Throwables.getStackTraceAsString(e));
            }
            return null;
        }

        protected void refreshBlock(int mark) throws IOException {
            _block.rewind();
            _offsetAtFile -= _block.limit() - mark;
            _block.limit(Math.min(_block.capacity(), getJournalLength() - _offsetAtFile));
            final int read = _fch.read(_block, _offsetAtFile);
            _offsetAtFile += read;
            _block.rewind();
        }

        public @Override boolean hasNext() {
            return _next != null;
        }

        public @Override BinDocument next() {
            Preconditions.checkState(_next != null);

            final BinDocument next = _next;
            _next = readNext();
            return next;
        }

        public @Override void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
