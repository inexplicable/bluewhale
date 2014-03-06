package org.ebaysf.bluewhale.storage;

import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.eventbus.EventBus;
import org.ebaysf.bluewhale.util.Files;
import org.ebaysf.bluewhale.util.Maps;
import org.javatuples.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

/**
 * Manages journals' Life cycles, mainly the DirectByteBuffers.
 */
public class JournalsManager {

	private static final Logger LOG = Logger.getLogger(JournalsManager.class.getName());

    private final File _local;
    private final int _journalLength;
    private final boolean _cleanUpOnExit;

    protected final EventBus _eventBus;
    protected final ExecutorService _executor;

    private final RemovalListener<ByteBuffer, BinJournal> _journalsNoLongUsedListener = new RemovalListener<ByteBuffer, BinJournal>() {
        public @Override void onRemoval(RemovalNotification<ByteBuffer, BinJournal> notification) {
			if(RemovalCause.COLLECTED.equals(notification.getCause())){
                final RemovalListener<ByteBuffer, BinJournal> watcher = _bufferRefsWatchers.get(notification.getKey());
                if(watcher != null){
                   watcher.onRemoval(notification);
                }
			}
		}
	};

    private final Map<ByteBuffer, RemovalListener<ByteBuffer, BinJournal>> _bufferRefsWatchers =
            Maps.INSTANCE.newIdentityMap();
	private final com.google.common.cache.Cache<ByteBuffer, BinJournal> _buffersUsedByJournals =
            Maps.INSTANCE.newIdentityWeakValuesCache(_journalsNoLongUsedListener);

    public JournalsManager(final File local,
                           final int journalLength,
                           final boolean cleanUpOnExit,
                           final EventBus eventBus,
                           final ExecutorService executor){

        _local = local;
        _journalLength = journalLength;
        _cleanUpOnExit = cleanUpOnExit;

        _eventBus = eventBus;
        _executor = executor;
    }

    public Pair<File, ByteBuffer> newBuffer() throws IOException {

        final File next = Files.newJournalFile(_local, _cleanUpOnExit);

        return new Pair<File, ByteBuffer>(next, com.google.common.io.Files.map(next, FileChannel.MapMode.READ_WRITE, _journalLength));
    }

    public void freeUpBuffer(final ByteBuffer buffer){

        _executor.submit(new FreeUpBufferTask(buffer));
    }

    /**
     * this is added to avoid premature releasing of the buffer, using weak reference journals, we could determine
     * when a ByteBuffer is ready to be recycled by querying whether its belonging journal has been nulled.
     *
     * every construction must inform this reference relationship in order to make it work.
     * @param buffer
     * @param journal
     */
    protected void rememberBufferUsedByJournal(final ByteBuffer buffer, final BinJournal journal){

        _buffersUsedByJournals.put(buffer, journal);
    }

    private class FreeUpBufferTask implements Runnable, RemovalListener<ByteBuffer, BinJournal> {

        private final ByteBuffer _buffer;

        public FreeUpBufferTask(final ByteBuffer buffer){

            _buffer = buffer;
            _bufferRefsWatchers.put(_buffer, this);
        }

        public @Override void run(){
            LOG.fine("[releasing][journal][buffer]");
            if(_buffersUsedByJournals.getIfPresent(_buffer) == null){
                freeUp();
            }
            else{
                LOG.fine("[release deferred][journal][buffer]" + _buffer);
            }
        }

        public @Override void onRemoval(RemovalNotification<ByteBuffer, BinJournal> notification) {

            if(notification.getKey() == _buffer){
                freeUp();
            }
        }

        private void freeUp() {

            _bufferRefsWatchers.remove(_buffer);

            Files.freeUpBuffer(_buffer);

            LOG.fine("[released][journal][buffer]");
        }
    }

}
