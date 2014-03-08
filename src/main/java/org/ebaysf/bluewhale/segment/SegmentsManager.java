package org.ebaysf.bluewhale.segment;

import com.google.common.base.Preconditions;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.ebaysf.bluewhale.util.Files;
import org.ebaysf.bluewhale.util.Maps;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manages Segments' Life cycles, mainly the DirectByteBuffers.
 */
public class SegmentsManager {

	private static final Logger LOG = LoggerFactory.getLogger(SegmentsManager.class);

    private final Configuration _configuration;
    private final int _spanAtLeast;
    private final Queue<Pair<File, ByteBuffer>> _availableBuffers;

    private final RemovalListener<Pair<File, ByteBuffer>, Segment> _segmentsNoLongerUsedListener = new RemovalListener<Pair<File, ByteBuffer>, Segment>() {
        public @Override void onRemoval(RemovalNotification<Pair<File, ByteBuffer>, Segment> notification) {
			if(RemovalCause.COLLECTED.equals(notification.getCause())){
                _bufferRefsWatchers.get(notification.getKey()).onRemoval(notification);
			}
		}
	};

    private final Map<Pair<File, ByteBuffer>, RemovalListener<Pair<File, ByteBuffer>, Segment>> _bufferRefsWatchers =
            Maps.INSTANCE.newIdentityMap();
	private final com.google.common.cache.Cache<Pair<File, ByteBuffer>, Segment> _buffersUsedBySegments =
            Maps.INSTANCE.newIdentityWeakValuesCache(_segmentsNoLongerUsedListener);

    public SegmentsManager(final Configuration configuration){

        _configuration = Preconditions.checkNotNull(configuration);
        _spanAtLeast = Math.max(1, Segment.MAX_SEGMENTS >> (configuration.getConcurrencyLevel() + configuration.getMaxSegmentDepth()));
        _availableBuffers = new ConcurrentLinkedQueue<Pair<File, ByteBuffer>>();

        LOG.debug("[segment manager] spanAtLeast: {}", _spanAtLeast);
    }

    public Pair<File, ByteBuffer> allocateBuffer() throws IOException {

        final Pair<File, ByteBuffer> available = _availableBuffers.poll();
        if(available == null){
            _configuration.getExecutor().submit(_allocateBufferAheadTask);
            return newBuffer();
        }
        else {
            return available;
        }
    }

	protected Pair<File, ByteBuffer> newBuffer() throws IOException {

        final File bufferFile = Files.newSegmentFile(_configuration.getLocal(), !_configuration.isPersistent());
        final ByteBuffer buffer = com.google.common.io.Files.map(bufferFile, FileChannel.MapMode.READ_WRITE, Segment.SIZE);
		resetTokens(buffer.asLongBuffer());
		return new Pair<File, ByteBuffer>(bufferFile, buffer);
	}

    protected Pair<File, ByteBuffer> loadBuffer(final File source) throws IOException {

        if(!_configuration.isPersistent()){
            source.deleteOnExit();
        }
        final ByteBuffer buffer = com.google.common.io.Files.map(source, FileChannel.MapMode.READ_WRITE, Segment.SIZE);
        return Pair.with(source, buffer);
    }

    public int getSpanAtLeast() {
        return _spanAtLeast;
    }

    public void freeUpBuffer(final Pair<File, ByteBuffer> buffer){

        _configuration.getExecutor().submit(new FreeUpBufferTask(buffer));
    }

    public RangeMap<Integer, Segment> initSegments(final List<Segment> coldSegments,
                                                   final BinStorage storage) throws IOException {

        final ImmutableRangeMap.Builder<Integer, Segment> builder = ImmutableRangeMap.builder();
        if(coldSegments.isEmpty()){
            final int span = Segment.MAX_SEGMENTS >> _configuration.getConcurrencyLevel();

            for(int lowerBound = 0, upperBound = lowerBound + span - 1; lowerBound < Segment.MAX_SEGMENTS; lowerBound += span, upperBound += span){
                final Range<Integer> range = Range.closed(lowerBound, upperBound);
                final Pair<File, ByteBuffer> allocate = allocateBuffer();
                builder.put(range,
                        new LeafSegment(allocate.getValue0(), range, _configuration, this, storage, allocate.getValue1(), 0));
            }
        }
        else{
            for(Segment loading: coldSegments){
                final Range<Integer> range = loading.range();
                final File source = loading.local();
                builder.put(range,
                        new LeafSegment(source, range, _configuration, this, storage, loadBuffer(source).getValue1(), loading.size()));
            }
        }
        return builder.build();
    }

    /**
     * set every long to -1L
     * @param buffer
     */
    protected static void resetTokens(final LongBuffer buffer){
        buffer.clear();
        while(buffer.hasRemaining()){
            buffer.put(-1L);
        }
        buffer.rewind();
    }

    /**
     * this is added to avoid premature releasing of the buffer, using weak reference segments, we could determine
     * when a ByteBuffer is ready to be recycled by querying whether its belonging segment has been nulled.
     *
     * every LeafSegment construction must inform this reference relationship in order to make it work.
     * @param buffer
     * @param segment
     */
    protected void rememberBufferUsedBySegment(final Pair<File, ByteBuffer> buffer, final Segment segment){

        _buffersUsedBySegments.put(buffer, segment);
    }


    private final Runnable _allocateBufferAheadTask = new Runnable() {
        public @Override void run() {
            if(SegmentsManager.this._availableBuffers.size() <= 1){
                try {
                    //offer 2 instead of one
                    LOG.debug("allocate segment buffer ahead");
                    SegmentsManager.this._availableBuffers.offer(newBuffer());
                    SegmentsManager.this._availableBuffers.offer(newBuffer());
                }
                catch (Exception e) {
                    LOG.error("allocate segment buffer ahead failed", e);
                }
            }
        }
    };

    private class FreeUpBufferTask implements Runnable, RemovalListener<Pair<File, ByteBuffer>, Segment> {

        private final Pair<File, ByteBuffer> _buffer;

        public FreeUpBufferTask(final Pair<File, ByteBuffer> buffer){

            _buffer = buffer;
            _bufferRefsWatchers.put(_buffer, this);
        }

        public @Override void run(){

            LOG.debug("releasing segment buffer");

            if(_buffersUsedBySegments.getIfPresent(_buffer) == null){
                freeUp();
            }
            else{
                LOG.debug("release segment buffer: {} delayed for segment still in use", _buffer);
            }
        }

        public @Override void onRemoval(RemovalNotification<Pair<File, ByteBuffer>, Segment> notification) {

            if(notification.getKey() == _buffer){
                freeUp();
            }
        }

        private void freeUp() {

            _bufferRefsWatchers.remove(_buffer);
            resetTokens(_buffer.getValue1().asLongBuffer());
            _availableBuffers.offer(_buffer);

            LOG.debug("release segment buffer: {} eventually", _buffer);
        }
    }

}
