package org.ebaysf.bluewhale.segment;

import com.google.common.base.Throwables;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.util.Files;
import org.ebaysf.bluewhale.util.Maps;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Manages Segments' Life cycles, mainly the DirectByteBuffers.
 */
public class SegmentsManager {

	private static final Logger LOG = Logger.getLogger(SegmentsManager.class.getName());

    private final File _local;
    private final int _spanAtLeast;
    private final Cache<?, ?> _belongsTo;
    private final Queue<ByteBuffer> _availableBuffers;

    private final RemovalListener<ByteBuffer, Segment> _segmentsNoLongerUsedListener = new RemovalListener<ByteBuffer, Segment>() {
        public @Override void onRemoval(RemovalNotification<ByteBuffer, Segment> notification) {
			if(RemovalCause.COLLECTED.equals(notification.getCause())){
                _bufferRefsWatchers.get(notification.getKey()).onRemoval(notification);
			}
		}
	};

    private final Map<ByteBuffer, RemovalListener<ByteBuffer, Segment>> _bufferRefsWatchers =
            Maps.INSTANCE.newIdentityMap();
	private final com.google.common.cache.Cache<ByteBuffer, Segment> _buffersUsedBySegments =
            Maps.INSTANCE.newIdentityWeakValuesCache(_segmentsNoLongerUsedListener);

    public SegmentsManager(final File local,
                           final int spanAtLeast,
                           final Cache<?, ?> belongsTo){

        _local = local;
        _spanAtLeast = Math.max(1, spanAtLeast);
        _belongsTo = belongsTo;
        _availableBuffers = new ConcurrentLinkedQueue<ByteBuffer>();
    }

    public ByteBuffer allocateBuffer() throws IOException {

        final ByteBuffer buffer = _availableBuffers.poll();
        if(buffer == null){
            _belongsTo.getExecutor().submit(_allocateBufferAheadTask);
            return newBuffer();
        }
        else {
            return buffer;
        }
    }

	protected ByteBuffer newBuffer() throws IOException {

        final File bufferFile = Files.newSegmentFile(_local);
        final ByteBuffer buffer = com.google.common.io.Files.map(bufferFile, FileChannel.MapMode.READ_WRITE, Segment.SIZE);
		resetTokens(buffer.asLongBuffer());
		return buffer;
	}

    public int getSpanAtLeast() {
        return _spanAtLeast;
    }

    public void freeUpBuffer(final ByteBuffer buffer){

        _belongsTo.getExecutor().submit(new FreeUpBufferTask(buffer));
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
    protected void rememberBufferUsedBySegment(final ByteBuffer buffer, final Segment segment){

        _buffersUsedBySegments.put(buffer, segment);
    }


    private final Runnable _allocateBufferAheadTask = new Runnable() {
        public @Override void run() {
            if(SegmentsManager.this._availableBuffers.size() <= 1){
                try {
                    //offer 2 instead of one
                    LOG.fine("[predicting]");
                    SegmentsManager.this._availableBuffers.offer(newBuffer());
                    SegmentsManager.this._availableBuffers.offer(newBuffer());
                }
                catch (Exception e) {
                    LOG.warning(Throwables.getStackTraceAsString(e));
                }
            }
        }
    };

    private class FreeUpBufferTask implements Runnable, RemovalListener<ByteBuffer, Segment> {

        private final ByteBuffer _buffer;

        public FreeUpBufferTask(final ByteBuffer buffer){

            _buffer = buffer;
            _bufferRefsWatchers.put(_buffer, this);
        }

        public @Override void run(){
            LOG.fine("[releasing][segment][buffer]");
            if(_buffersUsedBySegments.getIfPresent(_buffer) == null){
                freeUp();
            }
            else{
                LOG.fine("[release deferred][segment][buffer]" + _buffer);
            }
        }

        public @Override void onRemoval(RemovalNotification<ByteBuffer, Segment> notification) {

            if(notification.getKey() == _buffer){
                freeUp();
            }
        }

        private void freeUp() {

            _bufferRefsWatchers.remove(_buffer);
            resetTokens(_buffer.asLongBuffer());
            _availableBuffers.offer(_buffer);

            LOG.fine("[released][segment][buffer]");
        }
    }

}
